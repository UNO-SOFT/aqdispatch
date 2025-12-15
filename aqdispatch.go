// Copyright 2021, 2025 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

// Package aqdispatch implements multiple queues over a single Oracle AQ.
//
// https://docs.oracle.com/en/database/oracle/oracle-database/21/adque/aq-introduction.html#GUID-4237239B-1603-4F10-9B41-4D527994FDFA
package aqdispatch

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding"

	"github.com/godror/godror"
	"github.com/nsqio/go-diskqueue"
)

//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
//go:generate protoc --proto_path=pb --go_out=pb -I../../../ --go_opt=paths=source_relative pb/task.proto
//go:generate sed -i -e "/\"github.com\\/golang\\/protobuf\\/ptypes\\/timestamp\"/ s,\".*\",\"google.golang.org/protobuf/types/known/timestamppb\"," pb/task.pb.go

// Config of the Dispatcher.
type Config struct {

	// Enc is needed when DB sends/requires something other than UTF-8
	Enc encoding.Encoding

	*slog.Logger
	//Tracer:     tracer,

	// RequestKeyName is the attribute name instead of NAME.
	RequestKeyName string

	// DisQPrefix is the diskqueue file's prefix.
	DisQPrefix string
	// DisQPath is the path for the diskqueue.
	DisQPath string

	// Correlation specifies that only dequeue messages with the same correlation string.
	Correlation string
	// ResponseKeyPayload is the attribute name instead of PAYLOAD
	ResponseKeyPayload string
	// ResponseKeyBlob is the attribute name instead of AUX
	ResponseKeyBlob string

	// ResponseKeyErrMsg is the attribute name instead of ERRMSG
	ResponseKeyErrMsg string
	// RequestKeyPayload is the attribute name instead of PAYLOAD
	RequestKeyPayload string
	// RequestKeyBlob is the attribute name instead of AUX
	RequestKeyBlob string

	DisQMaxFileSize, DisQSyncEvery int64
	DisQSyncTimeout                time.Duration

	Timeout, PipeTimeout time.Duration
	// Concurrency is the number of concurrent RPCs.
	Concurrency int

	DisQMinMsgSize, DisQMaxMsgSize int32
}

// Task is a task.
//type Task = pb.Task

// DoFunc is the type of the function that processes the Task.
type DoFunc func(context.Context, io.Writer, Task) (io.Reader, error)

// New returns a new Dispatcher, which receives inQType typed messages on inQName.
//
// Then it calls "do" function with the task, and sends its output as response
// on outQName queue in outQType message.
//
// When outQNameand outQType is empty, no response is sent, no response queue is opened.
func New(
	db *sql.DB,
	conf Config,
	inQName, inQType string,
	do DoFunc,
	outQName, outQType string,
) (*Dispatcher, error) {
	if conf.RequestKeyName == "" {
		conf.RequestKeyName = "NAME"
	}
	if conf.RequestKeyPayload == "" {
		conf.RequestKeyPayload = "PAYLOAD"
	}
	if conf.RequestKeyBlob == "" {
		conf.RequestKeyBlob = "AUX"
	}
	if conf.ResponseKeyErrMsg == "" {
		conf.ResponseKeyErrMsg = "ERRMSG"
	}
	if conf.ResponseKeyPayload == "" {
		conf.ResponseKeyPayload = "PAYLOAD"
	}
	if conf.ResponseKeyBlob == "" {
		conf.ResponseKeyBlob = "AUX"
	}
	if conf.DisQPrefix == "" {
		conf.DisQPrefix = "aqdispatch-"
	}
	if conf.DisQPath == "" {
		conf.DisQPath = "."
	}
	// nosemgrep: go.lang.correctness.permissions.file_permission.incorrect-default-permission
	_ = os.MkdirAll(conf.DisQPath, 0750)
	if conf.DisQMinMsgSize <= 0 {
		conf.DisQMinMsgSize = 1
	}
	if conf.DisQMaxMsgSize <= 0 {
		conf.DisQMaxMsgSize = 16 << 20
	}
	if conf.DisQMaxFileSize <= 0 {
		conf.DisQMaxFileSize = 16 << 20
	}
	if conf.DisQSyncEvery <= 0 {
		conf.DisQSyncEvery = 1 << 10
	}
	if conf.DisQSyncTimeout <= 0 {
		conf.DisQSyncTimeout = time.Second
	}

	if conf.Timeout <= 0 {
		conf.Timeout = 30 * time.Second
	}
	if conf.PipeTimeout <= 0 {
		conf.PipeTimeout = 30 * time.Second
	}
	if conf.Concurrency <= 0 {
		conf.Concurrency = runtime.GOMAXPROCS(-1) * 16
	}
	if do == nil {
		return nil, errors.New("do function cannot be nil")
	}
	if inQName == "" || inQType == "" {
		return nil, errors.New("inQName, inQType are required")
	}
	if outQName != "" && outQType == "" || outQName == "" && outQType != "" {
		return nil, errors.New("outQName, outQType are required")
	}
	if conf.Logger == nil {
		conf.Logger = slog.New(slog.DiscardHandler)
	}
	di := Dispatcher{
		conf:    conf,
		do:      do,
		ins:     make(map[string]chanGateQ),
		datas:   make(chan *godror.Data, conf.Concurrency),
		buffers: make(chan *bytes.Buffer, conf.Concurrency),
		deqMsgs: make([]godror.Message, conf.Concurrency),
	}

	ctx := context.Background()

	getQ, err := newConnQueue(ctx, db, inQName, inQType, nil, func(opts *godror.DeqOptions) {
		opts.Mode = godror.DeqBrowse
		opts.Navigation = godror.NavFirst
		opts.Visibility = godror.VisibleImmediate
		opts.Wait = conf.PipeTimeout
		opts.Correlation = conf.Correlation
	})
	if err != nil {
		di.Close()
		return nil, err
	}
	// The remove queue is only required because we have to dequeue with different DeqOptions,
	// but on the same queue and on the same connection.
	rmQ, err := newConnQueue(ctx, db, inQName, inQType, nil, func(opts *godror.DeqOptions) {
		opts.Mode = godror.DeqPeek // Remove without data
		opts.Navigation = godror.NavFirst
		opts.Visibility = godror.VisibleImmediate
		opts.Wait = 0
		opts.Correlation = conf.Correlation
	})
	if err != nil {
		di.Close()
		return nil, err
	}
	di.rmQ = rmQ

	if err = godror.Raw(ctx, getQ.Cx, func(conn godror.Conn) error {
		di.Timezone = conn.Timezone()
		return nil
	}); err != nil {
		di.Close()
		return nil, err
	}
	di.getQ = getQ
	di.conf.Info("getQ", "name", di.getQ.Name())
	if err = di.getQ.PurgeExpired(ctx); err != nil {
		lvl := slog.LevelWarn
		var ec interface{ Code() int }
		if errors.As(err, &ec) && ec.Code() == 6550 {
			lvl = slog.LevelInfo
		}
		di.conf.Log(ctx, lvl, "PurgeExpired", "queue", di.getQ.Name(), "error", err)
	}
	_, di.getQHasBlob = di.getQ.PayloadObjectType().Attributes[di.conf.RequestKeyBlob]

	if outQName == "" {
		return &di, nil
	}
	di.putObjects = make(chan *godror.Object, conf.Concurrency)

	if di.putQ, err = newConnQueue(ctx, db, outQName, outQType, func(opts *godror.EnqOptions) {
		opts.Visibility = godror.VisibleImmediate
	}, nil); err != nil {
		di.Close()
		return nil, err
	}
	di.conf.Info("putQ", "name", di.putQ.Name())
	if err = di.putQ.PurgeExpired(ctx); err != nil {
		di.conf.Warn("PurgeExpired", "queue", di.putQ.Name(), "error", err)
	}
	_, di.putQHasBlob = di.putQ.PayloadObjectType().Attributes[di.conf.ResponseKeyBlob]
	return &di, nil
}

// PurgeExpired calls PurgeExpired on the underlying queues,
// purging expired messages.
func (di *Dispatcher) PurgeExpired(ctx context.Context) error {
	var firstErr error
	if !di.getQ.IsZero() {
		firstErr = di.getQ.PurgeExpired(ctx)
	}
	if !di.putQ.IsZero() {
		if err := di.putQ.PurgeExpired(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Run the dispatcher, accepting tasks with names in taskNames.
func (di *Dispatcher) Run(ctx context.Context) error {
	defer func() { di.conf.Info("Run finished") }()

	// Find already existing disk queues
	prefix := filepath.Join(di.conf.DisQPath, di.conf.DisQPrefix)
	names, err := filepath.Glob(prefix + "*.diskqueue.meta.dat")
	if err != nil {
		return err
	}
	for i, fn := range names {
		nm, _, ok := strings.Cut(strings.TrimPrefix(fn, prefix), ".diskqueue")
		if !ok {
			nm = ""
		}
		names[i] = nm
		if nm != "" {
			di.conf.Info("found", "file", fn, "name", nm)
		}
	}
	di.mu.Lock()
	di.consumerGrp, di.consumerCtx = errgroup.WithContext(ctx)
	ctx = di.consumerCtx
	for _, nm := range names {
		if nm != "" {
			// Start existing disk queue's consumption
			di.newIn(nm)
		}
	}
	di.mu.Unlock()

	timer := time.NewTimer(di.conf.PipeTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := di.batch(ctx); err != nil {
			if err == io.EOF {
				continue
			} else if errors.Is(err, ErrBadSetup) {
				return err
			}
			var ec interface{ Code() int }
			di.conf.Error("batch finished", "error", err)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || godror.IsBadConn(err) {
				return err
			} else if errors.As(err, &ec) {
				switch ec.Code() {
				case 24010, // ORA-24010: Queue does not exist
					25226: // ORA-25226: dequeue failed, queue is not enabled for dequeue
					return err
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			// Wait 10s before trying again
			timer.Reset(di.conf.PipeTimeout)
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// Dispatcher. After creating with New, run it with Run.
//
// Reads tasks and store the messages in diskqueues - one for each distinct NAME.
// If Concurrency allows, calls the do function given in New,
// and sends the answer as PAYLOAD of what that writes as response.
type Dispatcher struct {
	putConn                  godror.Conn
	putObjects               chan *godror.Object
	getQ, putQ, rmQ          *connQueue
	ins                      map[string]chanGateQ
	datas                    chan *godror.Data
	do                       DoFunc
	buffers                  chan *bytes.Buffer
	Timezone                 *time.Location
	deqMsgs                  []godror.Message
	conf                     Config
	mu                       sync.RWMutex
	inProgress               sync.WaitGroup
	consumerCtx              context.Context
	consumerGrp              *errgroup.Group
	getQHasBlob, putQHasBlob bool
}

type chanGateQ struct {
	name string
	ch   chan Task
	q    diskqueue.Interface
	gate chan struct{}
}

func newChanGateQ(conf Config, nm string) chanGateQ {
	diskQLogger := conf.Logger.With("lib", "diskqueue", "nm", nm)
	return chanGateQ{
		name: nm,
		ch:   make(chan Task),
		gate: make(chan struct{}, conf.Concurrency),

		q: diskqueue.New(conf.DisQPrefix+nm, conf.DisQPath,
			conf.DisQMaxFileSize, conf.DisQMinMsgSize, conf.DisQMaxMsgSize,
			conf.DisQSyncEvery, conf.DisQSyncTimeout,
			func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
				if diskQLogger != nil && lvl >= diskqueue.INFO {
					diskQLogger.Info(fmt.Sprintf(f, args...))
				}
			}),
	}
}

func (di *Dispatcher) newIn(nm string) chanGateQ {
	in := newChanGateQ(di.conf, nm)
	di.ins[nm] = in
	di.consumerGrp.Go(func() error {
		return di.consume(di.consumerCtx, in)
	})
	return in
}

// Decode the string from DB's encoding to UTF-8.
func (di *Dispatcher) Decode(p []byte) string {
	if di.conf.Enc == nil {
		return string(p)
	}
	q, _ := di.conf.Enc.NewDecoder().Bytes(p)
	return string(q)
}

// Encode a string using the DB's encoding.
func (di *Dispatcher) Encode(s string) string {
	if di.conf.Enc == nil {
		return s
	}
	s, _ = encoding.ReplaceUnsupported(di.conf.Enc.NewEncoder()).String(s)
	return s
}

// Close the dispatcher.
func (di *Dispatcher) Close() error {
	di.inProgress.Wait()
	di.mu.Lock()
	defer di.mu.Unlock()
	ins, putO := di.ins, di.putObjects
	di.ins, di.putObjects = nil, nil
	getQ, putQ, rmQ := di.getQ, di.putQ, di.rmQ
	di.getQ, di.putQ, di.rmQ = nil, nil, nil
	di.putConn = nil
	for _, c := range ins {
		if c.ch != nil {
			close(c.ch)
		}
		if c.q != nil {
			c.q.Close()
		}
	}
	if putO != nil {
		close(putO)
		for obj := range putO {
			if obj != nil {
				obj.Close()
			}
		}
	}
	if getQ != nil {
		getQ.Close()
	}
	if putQ != nil {
		putQ.Close()
	}
	if rmQ != nil {
		rmQ.Close()
	}

	return nil
}

var (
	ErrUnknownCommand = errors.New("unknown command")
	ErrSkipResponse   = errors.New("skip response")
	ErrEmpty          = errors.New("empty")
	ErrExit           = errors.New("exit")
	ErrAnswer         = errors.New("answer send error")
	ErrBadSetup       = errors.New("bad setup")

	errChannelClosed = errors.New("channel is closed")
	errContinue      = errors.New("continue")
)

// batch process at most Config.Concurrency number of messages: wait at max Config.Timeout,
// then for each message, decode it, send to the named channel if possible,
// otherwise save it to a specific diskqueue.
func (di *Dispatcher) batch(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	di.mu.RLock()
	msgs, conf := di.deqMsgs[:], di.conf
	di.mu.RUnlock()

	rOpts, err := di.rmQ.DeqOptions()
	if err != nil {
		return err
	}

	n, deqErr := di.getQ.Dequeue(msgs)
	conf.Debug("dequeue", "n", n, "error", deqErr)
	msgs = msgs[:n]

	var b []byte
	var firstErr error
	delMsgs := make([]godror.Message, 1)
	one := func(ctx context.Context, msg *godror.Message) error {
		var task Task
		// The messages are tightly coupled with the queue,
		// so we must parse them sequentially.
		err := di.parse(ctx, &task, msg)
		if err != nil {
			conf.Warn("parse", "error", err)
			if errors.Is(err, ErrEmpty) {
				return fmt.Errorf("%w: %w", errContinue, err)
			}
			return fmt.Errorf("%w: parse: %w", errContinue, err)
		} else if task.IsZero() {
			panic("zero task no error")
		}
		conf.Debug("parsed", "task", task)

		nm := task.Name
		di.mu.RLock()
		in, ok := di.ins[nm]
		di.mu.RUnlock()
		if !ok {
			var ok bool
			di.mu.Lock()
			if in, ok = di.ins[nm]; !ok {
				// Create new queue
				in = di.newIn(nm)
			}
			di.mu.Unlock()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case in.ch <- task:
			// Success
		default:
			// conf.Concurrency processing is all occupied, so save it for future processing
			// diskQs are for traffic jams
			b = MarshalProtobuf(b[:0], task)
			conf.Info("enqueue", "task", task.Name, "deadline", task.Deadline) //task.GetDeadline().AsTime())
			if err = in.q.Put(b); err != nil {
				conf.Error("enqueue", "queue", task.Name, "error", err)
				var ec interface{ Code() int }
				if errors.As(err, &ec) && ec.Code() == 1031 { // insufficient privileges
					err = fmt.Errorf("%w: %w", ErrBadSetup, err)
				}
				return fmt.Errorf("put surplus task into %q queue: %w", task.Name, err)
			}
		}

		// remove successfully processed message from queue
		rOpts.MsgID = append(rOpts.MsgID[:0], msg.MsgID[:]...)
		conf.Debug("rm", "msgID", rOpts.MsgID)
		if n, err := di.rmQ.DequeueWithOptions(delMsgs, &rOpts); err != nil {
			return fmt.Errorf("rm Dequeue %v: %w", msg.MsgID, err)
		} else if n == 0 {
			conf.Warn("remove failed", "msgID", msg.MsgID)
		}
		return nil
	}

	for i := range msgs {
		if err := func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(ctx, conf.Timeout)
			defer cancel()

			err := one(ctx, &msgs[i])
			if err != nil {
				lvl := slog.LevelError
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					lvl = slog.LevelInfo
				} else if errors.Is(err, errContinue) {
					lvl = slog.LevelWarn
				}
				conf.Log(ctx, lvl, "one", "error", err)
				if lvl != slog.LevelError {
					err = nil
				}
			}
			return err
		}(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		conf.Error("batch", "error", firstErr)
		return firstErr
	} else if deqErr != nil {
		conf.Error("dequeue", "error", deqErr)
		var ec interface{ Code() int }
		if errors.As(deqErr, &ec) && ec.Code() == 1031 { // insufficient privileges
			deqErr = fmt.Errorf("%w: %w", ErrBadSetup, deqErr)
		}
		return deqErr
	}
	return nil
}

// consume the named queue. Does not return.
//
// MUST be started for each function name!
//
// First it processes the diskqueue (old, saved messages),
// then waits on the "nm" named channel for tasks.
//
// When nm is the empty string, that's a catch-all (not catched by others).
func (di *Dispatcher) consume(ctx context.Context, in chanGateQ) error {
	var qCh <-chan []byte
	if in.q != nil {
		qCh = in.q.ReadChan()
	}
	one := func() (Task, *slog.Logger, error) {
		var task Task
		var ok bool
		var source string
		logger := di.conf.Logger
		select {
		case <-ctx.Done():
			return task, logger, ctx.Err()
		case task, ok = <-in.ch:
			if !ok {
				return task, logger, fmt.Errorf("%q: %w", in.name, errChannelClosed)
			}
			source = "in"
		case b, ok := <-qCh:
			if !ok {
				return task, logger, nil
			}
			if err := task.UnmarshalProtobuf(b); err != nil {
				di.conf.Error("unmarshal", "bytes", b, "error", err)
				return task, logger, errContinue
			}
			source = "q"
		}
		logger = logger.With(
			slog.String("source", source+"Ch["+in.name+"]"))
		di.conf.Info("consume", "task", task)
		if task.IsZero() {
			return task, logger, errContinue
		}
		logger = di.conf.With(
			slog.String("name", task.Name),
			slog.String("refID", task.RefID))
		if task.Name == "" {
			logger.Info("empty task", "task", task)
			return task, logger, errContinue
		}
		deadline := task.Deadline
		if deadline.IsZero() {
			deadline = time.Now().Add(di.conf.Timeout)
		}
		if !deadline.After(time.Now()) {
			logger.Info("skip overtime", slog.Time("deadline", deadline), slog.String("refID", task.RefID))
			if task.RefID != "" {
				go di.answer(task.RefID, nil, nil, context.DeadlineExceeded)
			}
			return task, logger, errContinue
		}

		logger = logger.With(slog.Time("deadline", deadline))
		logger.Info("begin",
			slog.Int("payloadLen", len(task.Payload)),
			slog.Int("blobsCount", len(task.Blobs)))
		return task, logger, nil
	}
	oneGated := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case in.gate <- struct{}{}:
			task, logger, err := one()
			if err != nil || task.IsZero() {
				<-in.gate
				return err
			}

			di.inProgress.Add(1)
			// Execute on a separate goroutine
			go func() {
				defer func() { di.inProgress.Done(); <-in.gate }()
				logger.Debug("begin execute")
				di.execute(ctx, task)
				logger.Info("executed")
			}()
			return nil
		}
	}

	for {
		if err := oneGated(); err == nil ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, errContinue) {
			continue
		} else if errors.Is(err, context.Canceled) {
			return nil
		} else {
			return err
		}
	}
}

// parse a *godror.Message from the queue into a Task.
func (di *Dispatcher) parse(ctx context.Context, task *Task, msg *godror.Message) error {
	if msg != nil && msg.Object != nil {
		defer func() {
			if msg != nil && msg.Object != nil {
				msg.Object.Close()
			}
		}()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var data *godror.Data
	select {
	case data = <-di.datas:
	default:
		var x godror.Data
		data = &x
	}
	defer func() {
		select {
		case di.datas <- data:
		default:
		}
	}()
	task.RefID = msg.Correlation
	deadline := msg.Deadline()
	if !deadline.IsZero() {
		deadline = deadline.Add(-1 * time.Second)
	} else {
		deadline = time.Now().Add(di.conf.Timeout)
	}
	//task.Deadline = timestamppb.New(deadline)
	task.Deadline = deadline
	if task.RefID == "" {
		task.RefID = fmt.Sprintf("%X", msg.MsgID[:])
	}
	logger := di.conf.Logger.With("refID", task.RefID)
	obj := msg.Object
	logger.Debug("received", "msg", msg, "obj", obj)
	if err := obj.GetAttribute(data, di.conf.RequestKeyName); err != nil {
		logger.Error("GetAttribute", "key", di.conf.RequestKeyName, "error", err)
		return fmt.Errorf("get %s: %w", di.conf.RequestKeyName, err)
	}
	task.Name = string(data.GetBytes())
	logger.Debug("get task name", "data", data, "name", task.Name)

	if err := obj.GetAttribute(data, di.conf.RequestKeyPayload); err != nil {
		logger.Error("GetAttribute", "key", di.conf.RequestKeyPayload, "error", err)
		return fmt.Errorf("get %s: %w", di.conf.RequestKeyPayload, err)
	}
	logger.Debug("get payload", "data", data)
	var err error
	lob := data.GetLob()
	// This asserts that lob is a BLOB!
	size, err := lob.Size()
	if err != nil {
		return fmt.Errorf("getLOB: %w", err)
	}
	task.Payload = make([]byte, int(size))
	n, err := io.ReadFull(lob, task.Payload)
	task.Payload = task.Payload[:n]
	if err != nil {
		logger.Error("lob.ReadFull", "error", err)
		return fmt.Errorf("getLOB: %w", err)
	}

	if di.getQHasBlob {
		if err := obj.GetAttribute(data, di.conf.RequestKeyBlob); err != nil {
			logger.Error("GetAttribute", "key", di.conf.RequestKeyBlob, "error", err)
			return fmt.Errorf("get %s: %w", di.conf.RequestKeyBlob, err)
		}
		if !data.IsNull() {
			if lob = data.GetLob(); lob != nil {
				for {
					b := make([]byte, 1<<20)
					if n, err := io.ReadAtLeast(lob, b, 1<<19); n > 0 {
						task.Blobs = append(task.Blobs, &Blob{Bytes: b[:n]}) //&pb.Blob{Bytes: b[:n]})
					} else if err == nil {
						continue
					} else if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
						break
					} else {
						logger.Error("read LOB", "error", err)
						return err
					}
				}
			}
		}
	}
	lvl := slog.LevelInfo
	if task.Name == "" {
		lvl = slog.LevelWarn
	}

	logger.Log(ctx, lvl, "parse",
		slog.String("name", task.Name), slog.String("refID", task.RefID),
		slog.Int("payloadLen", len(task.Payload)), slog.Int("blobCount", len(task.Blobs)),
		slog.Time("enqueued", msg.Enqueued), slog.Time("deadline", deadline),
		slog.Duration("delay", msg.Delay), slog.Duration("expiry", msg.Expiration),
	)
	if task.RefID == "" || task.Name == "" {
		return ErrEmpty
	}

	return nil
}

// execute calls do on the task, then calls answer with the answer.
func (di *Dispatcher) execute(ctx context.Context, task Task) {
	di.conf.Debug("execute", "task", task)
	if task.RefID == "" || task.Name == "" {
		di.conf.Info("execute skip empty task", "task", task)
		return
	}

	logger := di.conf.Logger.With("refID", task.RefID)
	deadline := task.Deadline
	//if dl := task.GetDeadline(); dl.IsValid() {
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	logger.Info("execute", "name", task.Name, "payloadLen", len(task.Payload), "deadline", deadline)
	logger.Debug("execute", "payload", string(task.Payload))
	/*
		if di.Tracer != nil {
			tCtx, span := di.Tracer.Start(ctx, "task="+task.Name)
			ctx = tCtx
			defer span.End()
		}
	*/

	var res *bytes.Buffer
	select {
	case res = <-di.buffers:
		res.Reset()
	default:
		var x bytes.Buffer
		res = &x
	}
	defer func() {
		res.Reset()
		select {
		case di.buffers <- res:
		default:
		}
	}()
	if err := ctx.Err(); err != nil {
		return
	}
	start := time.Now()
	r, callErr := di.do(ctx, res, task)
	if callErr != nil {
		logger.Error("call", "dur", time.Since(start).String(), "error", callErr)
	} else {
		logger.Info("call", "dur", time.Since(start).String())
	}
	if errors.Is(callErr, ErrExit) || di.putQ.IsZero() {
		return
	}
	start = time.Now()
	err := di.answer(task.RefID, res.Bytes(), r, callErr)
	logger.Info("answer", "length", res.Len(), "dur", time.Since(start).String(), "error", err)
}

// answer puts the answer into the queue.
func (di *Dispatcher) answer(refID string, payload []byte, blob io.Reader, err error) error {
	di.conf.Debug("answer", "refID", refID, "payload", payload, "error", err)
	var errMsg string
	if err != nil {
		if errors.Is(err, ErrSkipResponse) {
			return nil
		}
		errMsg = err.Error()
		if len(errMsg) > 1000 {
			errMsg = errMsg[:1000]
		}
	}
	logger := di.conf.Logger.With("refID", refID)
	logger.Info("answer", "errMsg", errMsg)
	di.conf.Debug("answer", "payload", string(payload))

	var obj *godror.Object
Loop:
	for {
		obj = nil
		var ok bool
		select {
		case obj, ok = <-di.putObjects:
			if obj != nil {
				if err = obj.ResetAttributes(); err == nil {
					break Loop
				}
				obj.Close()
				obj = nil
			}
			if !ok {
				break Loop
			}
		default:
			break Loop
		}
	}
	if obj == nil {
		di.conf.Debug("create", "object", di.putQ.PayloadObjectType)
		if obj, err = di.putQ.PayloadObjectType().NewObject(); err != nil {
			return err
		}
	}
	defer func() {
		if err = obj.ResetAttributes(); err != nil {
			obj.Close()
			return
		}
		select {
		case di.putObjects <- obj:
		default:
			obj.Close()
		}
	}()

	if errMsg != "" {
		if err = obj.Set(di.conf.ResponseKeyErrMsg, errMsg); err != nil {
			obj.Close()
			return fmt.Errorf("set %s: %w", di.conf.ResponseKeyErrMsg, err)
		}
	}
	if len(payload) != 0 {
		if err = obj.Set(di.conf.ResponseKeyPayload, payload); err != nil {
			obj.Close()
			return fmt.Errorf("set %s: %w", di.conf.ResponseKeyPayload, err)
		}
	}
	if di.putQHasBlob && blob != nil {
		b := make([]byte, 1<<20)
		if n, err := io.ReadAtLeast(blob, b, 1<<19); n != 0 {
			if err != nil { // this is all
				if err = obj.Set(di.conf.ResponseKeyBlob, b[:n]); err != nil {
					return fmt.Errorf("set %s: %w", di.conf.ResponseKeyBlob, err)
				}
			} else {
				lob, err := di.putConn.NewTempLob(false)
				if err != nil {
					obj.Close()
					return fmt.Errorf("create temp lob: %w", err)
				}
				defer lob.Close()
				if _, err := io.Copy(io.NewOffsetWriter(lob, 0), blob); err != nil {
					obj.Close()
					return fmt.Errorf("write temp lob: %w", err)
				}
				if err = obj.Set(di.conf.ResponseKeyBlob, lob); err != nil {
					obj.Close()
					return fmt.Errorf("set %s: %w", di.conf.ResponseKeyBlob, err)
				}
			}
		}
	}

	msg := godror.Message{
		Correlation: refID,
		Expiration:  di.conf.Timeout / time.Second,
		Object:      obj,
	}
	if err = di.putQ.Enqueue([]godror.Message{msg}); err != nil {
		return err
	}
	return nil
}

func dbCtx(ctx context.Context, module, action string) context.Context {
	return godror.ContextWithTraceTag(ctx, godror.TraceTag{Module: module, Action: action})
}

type connQueue struct {
	q    *godror.Queue
	Cx   *sql.Conn
	init func() error
}

func newConnQueue(ctx context.Context, db *sql.DB, qName, qType string,
	mkEnqOpts func(*godror.EnqOptions), mkDeqOpts func(*godror.DeqOptions),
) (*connQueue, error) {
	var cq connQueue

	cq.init = func() error {
		cq.Close()
		if err := func() error {
			var err error
			if cq.Cx, err = db.Conn(dbCtx(ctx, "aqdispatch", qName)); err != nil {
				return err
			}
			if cq.q, err = godror.NewQueue(ctx, cq.Cx, qName, qType); err != nil {
				return err
			}

			if mkEnqOpts != nil {
				opts, err := cq.q.EnqOptions()
				if err != nil {
					return err
				}
				mkEnqOpts(&opts)
				if err = cq.q.SetEnqOptions(opts); err != nil {
					return err
				}
			}

			if mkDeqOpts != nil {
				opts, err := cq.q.DeqOptions()
				if err != nil {
					return err
				}
				mkDeqOpts(&opts)
				if err = cq.q.SetDeqOptions(opts); err != nil {
					return err
				}
			}
			return nil
		}(); err != nil {
			cq.Close()
			return err
		}
		return nil
	}

	if err := cq.init(); err != nil {
		return nil, err
	}
	return &cq, nil
}

func (cq *connQueue) Close() error {
	if cq == nil {
		return nil
	}
	if cq.q != nil {
		cq.q.Close()
	}
	if cq.Cx != nil {
		cq.Cx.Close()
	}
	cq.q, cq.Cx = nil, nil
	return nil
}
func (cq *connQueue) withRetry(f func() error) error {
	err := f()
	if err == nil {
		return nil
	}
	if !godror.IsBadConn(err) {
		return err
	}
	if err := cq.init(); err != nil {
		return err
	}
	return f()
}
func (cq *connQueue) IsZero() bool { return cq == nil || cq.Cx == nil }
func (cq *connQueue) Name() string { return cq.q.Name() }

func (cq *connQueue) DeqOptions() (opts godror.DeqOptions, err error) {
	err = cq.withRetry(func() error { opts, err = cq.q.DeqOptions(); return err })
	return opts, err
}
func (cq *connQueue) SetDeqOptions(opts godror.DeqOptions) error {
	return cq.withRetry(func() error { return cq.q.SetDeqOptions(opts) })
}
func (cq *connQueue) PayloadObjectType() *godror.ObjectType { return cq.q.PayloadObjectType }
func (cq *connQueue) PurgeExpired(ctx context.Context) error {
	return cq.withRetry(func() error { return cq.q.PurgeExpired(ctx) })
}
func (cq *connQueue) Enqueue(msgs []godror.Message) error {
	return cq.withRetry(func() error { return cq.q.Enqueue(msgs) })
}
func (cq *connQueue) EnqueueWithOptions(msgs []godror.Message, opts *godror.EnqOptions) error {
	return cq.withRetry(func() error { return cq.q.EnqueueWithOptions(msgs, opts) })
}
func (cq *connQueue) Dequeue(msgs []godror.Message) (n int, err error) {
	err = cq.withRetry(func() error { n, err = cq.q.Dequeue(msgs); return err })
	return n, err
}
func (cq *connQueue) DequeueWithOptions(msgs []godror.Message, opts *godror.DeqOptions) (n int, err error) {
	err = cq.withRetry(func() error { n, err = cq.q.DequeueWithOptions(msgs, opts); return err })
	return n, err
}
