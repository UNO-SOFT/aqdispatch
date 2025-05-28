// Copyright 2021, 2024 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

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
	"runtime"
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
	// QueueCount is the approximate number of queues dispatched over this AQ.
	QueueCount int
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
		conf.Concurrency = runtime.GOMAXPROCS(-1)
	}
	if conf.QueueCount <= 0 {
		conf.QueueCount = 1
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
		ins:     make(map[string]chan Task, conf.QueueCount),
		diskQs:  make(map[string]diskqueue.Interface, conf.QueueCount),
		gates:   make(map[string]chan struct{}, conf.QueueCount),
		datas:   make(chan *godror.Data, conf.Concurrency),
		buffers: make(chan *bytes.Buffer, conf.Concurrency),
		deqMsgs: make([]godror.Message, conf.Concurrency),
	}

	ctx := context.Background()
	getCx, err := db.Conn(dbCtx(ctx, "aqdispatch", inQName))
	if err != nil {
		di.Close()
		return nil, err
	}
	di.getCx = getCx

	if err = godror.Raw(ctx, getCx, func(conn godror.Conn) error {
		di.Timezone = conn.Timezone()
		return nil
	}); err != nil {
		di.Close()
		return nil, err
	}
	getQ, err := godror.NewQueue(ctx, getCx, inQName, inQType)
	if err != nil {
		di.Close()
		return nil, err
	}
	di.getQ = getQ
	di.conf.Info("getQ", "name", di.getQ.Name())
	if err = di.getQ.PurgeExpired(ctx); err != nil {
		di.conf.Warn("PurgeExpired", "queue", di.getQ.Name(), "error", err)
	}
	dOpts, err := di.getQ.DeqOptions()
	if err != nil {
		di.Close()
		return nil, err
	}
	dOpts.Mode = godror.DeqRemove
	dOpts.Navigation = godror.NavFirst
	dOpts.Visibility = godror.VisibleImmediate
	dOpts.Wait = conf.PipeTimeout
	dOpts.Correlation = conf.Correlation
	if err = di.getQ.SetDeqOptions(dOpts); err != nil {
		di.Close()
		return nil, err
	}
	_, di.getQHasBlob = getQ.PayloadObjectType.Attributes[di.conf.RequestKeyBlob]

	if outQName == "" {
		return &di, nil
	}

	di.putObjects = make(chan *godror.Object, conf.Concurrency)
	putCx, err := db.Conn(dbCtx(ctx, "aqdispatch", outQName))
	if err != nil {
		di.Close()
		return nil, err
	}
	di.putCx = putCx
	if di.putConn, err = godror.DriverConn(ctx, putCx); err != nil {
		di.Close()
		return nil, err
	}
	putQ, err := godror.NewQueue(ctx, putCx, outQName, outQType)
	if err != nil {
		di.Close()
		return nil, err
	}
	di.putQ = putQ
	di.conf.Info("putQ", "name", di.putQ.Name())
	if err = di.putQ.PurgeExpired(ctx); err != nil {
		di.conf.Warn("PurgeExpired", "queue", di.putQ.Name(), "error", err)
	}
	eOpts, err := di.putQ.EnqOptions()
	if err != nil {
		di.Close()
		return nil, err
	}
	eOpts.Visibility = godror.VisibleImmediate
	if err = di.putQ.SetEnqOptions(eOpts); err != nil {
		di.Close()
		return nil, err
	}
	_, di.putQHasBlob = putQ.PayloadObjectType.Attributes[di.conf.ResponseKeyBlob]
	return &di, nil
}

// PurgeExpired calls PurgeExpired on the underlying queues,
// purging expired messages.
func (di *Dispatcher) PurgeExpired(ctx context.Context) error {
	var firstErr error
	if di.getQ != nil {
		firstErr = di.getQ.PurgeExpired(ctx)
	}
	if di.putQ != nil {
		if err := di.putQ.PurgeExpired(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Run the dispatcher, accepting tasks with names in taskNames.
func (di *Dispatcher) Run(ctx context.Context, taskNames []string) error {
	defer func() { di.conf.Info("Run finished") }()
	grp, ctx := errgroup.WithContext(ctx)
	for _, nm := range taskNames {
		nm := nm
		grp.Go(func() error { return di.consume(ctx, nm, len(taskNames) <= 1) })
	}
	di.conf.Debug("prepared consumers", "taskNames", taskNames)
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
	getCx, putCx             io.Closer
	putConn                  godror.Conn
	gates                    map[string]chan struct{}
	putObjects               chan *godror.Object
	getQ                     *godror.Queue
	ins                      map[string]chan Task
	diskQs                   map[string]diskqueue.Interface
	putQ                     *godror.Queue
	datas                    chan *godror.Data
	do                       DoFunc
	buffers                  chan *bytes.Buffer
	Timezone                 *time.Location
	deqMsgs                  []godror.Message
	conf                     Config
	mu                       sync.RWMutex
	getQHasBlob, putQHasBlob bool
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
	di.mu.Lock()
	defer di.mu.Unlock()
	ins, putO, diskQs, getQ, putQ := di.ins, di.putObjects, di.diskQs, di.getQ, di.putQ
	di.ins, di.putObjects, di.diskQs, di.getQ, di.putQ = nil, nil, nil, nil, nil
	di.putConn = nil
	for _, c := range ins {
		close(c)
	}
	if putO != nil {
		close(putO)
		for obj := range putO {
			if obj != nil {
				obj.Close()
			}
		}
	}
	for _, q := range diskQs {
		if q != nil {
			q.Close()
		}
	}
	getCx, putCx := di.getCx, di.putCx
	di.getCx, di.putCx = nil, nil
	if putCx != nil {
		putCx.Close()
	}
	if getCx != nil {
		getCx.Close()
	}
	if getQ != nil {
		getQ.Close()
	}
	if putQ != nil {
		putQ.Close()
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
	n, err := di.getQ.Dequeue(msgs)
	if err != nil {
		conf.Error("dequeue", "error", err)
		var ec interface{ Code() int }
		if errors.As(err, &ec) && ec.Code() == 1031 { // insufficient privileges
			err = fmt.Errorf("%w: %w", ErrBadSetup, err)
		}
		return err
	}
	conf.Debug("dequeue", "n", n)
	if n == 0 {
		return nil
	}
	msgs = msgs[:n]

	var b []byte
	var firstErr error
	one := func(ctx context.Context, task Task, msg *godror.Message) error {
		// The messages are tightly coupled with the queue,
		// so we must parse them sequentially.
		err = di.parse(ctx, &task, msg)
		if err != nil {
			conf.Warn("parse", "error", err)
			if errors.Is(err, ErrEmpty) {
				return errContinue
			}
			if firstErr == nil {
				firstErr = err
			}
			return errContinue
		}
		conf.Debug("parsed", "task", task)

		nm := task.Name
		di.mu.RLock()
		inCh, ok := di.ins[nm]
		if !ok {
			conf.Warn("unknown task", "name", nm)
			if inCh, ok = di.ins[""]; ok {
				nm = ""
			}
		}
		q := di.diskQs[nm]
		di.mu.RUnlock()
		if !ok {
			if firstErr == nil {
				conf.Error("unknown task", "name", nm, "task", task)
				firstErr = fmt.Errorf("%w: %q (task=%q)", ErrUnknownCommand, nm, task.Name)
			}
			return errContinue
		}
		if q == nil {
			// Skip the disk queue, wait for the channel
			select {
			case <-ctx.Done():
				return ctx.Err()
			case inCh <- task:
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case inCh <- task:
			// Success
		default:
			// diskQs are for traffic jams
			b = MarshalProtobuf(b[:0], task)
			conf.Info("enqueue", "task", task.Name, "deadline", task.Deadline) //task.GetDeadline().AsTime())
			if err = q.Put(b); err != nil {
				conf.Error("enqueue", "queue", task.Name, "error", err)
				var ec interface{ Code() int }
				if errors.As(err, &ec) && ec.Code() == 1031 { // insufficient privileges
					err = fmt.Errorf("%w: %w", ErrBadSetup, err)
				}
				return fmt.Errorf("put surplus task into %q queue: %w", task.Name, err)
			}
			return errContinue // release Task
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

			// task := taskPool.Acquire()
			// defer taskPool.Release(task)
			var task Task
			err := one(ctx, task, &msgs[i])
			if err != nil {
				lvl := slog.LevelError
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errContinue) {
					lvl = slog.LevelInfo
				} else if !errors.Is(err, errContinue) {
					lvl = slog.LevelWarn
				}
				conf.Log(ctx, lvl, "one", "task", task, "error", err)
				if lvl != slog.LevelError {
					err = nil
				}
			} else {
				conf.Info("received", "task", task)
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
	}
	return firstErr
}

// consume the named queue. Does not return.
//
// MUST be started for each function name!
//
// First it processes the diskqueue (old, saved messages),
// then waits on the "nm" named channel for tasks.
//
// When nm is the empty string, that's a catch-all (not catched by others).
func (di *Dispatcher) consume(ctx context.Context, nm string, noDisQ bool) error {
	di.conf.Debug("consume", "name", nm)
	di.mu.RLock()
	inCh, gate, q := di.ins[nm], di.gates[nm], di.diskQs[nm]
	di.mu.RUnlock()
	if inCh == nil || gate == nil || q == nil && !noDisQ {
		di.mu.Lock()
		inCh, gate, q = di.ins[nm], di.gates[nm], di.diskQs[nm]
		if inCh == nil {
			inCh = make(chan Task)
			di.ins[nm] = inCh
		}
		if gate == nil {
			gate = make(chan struct{}, di.conf.Concurrency)
			di.gates[nm] = gate
		}
		if q == nil && !noDisQ {
			diskQLogger := di.conf.Logger.With("lib", "diskqueue", "nm", nm)
			q = diskqueue.New(di.conf.DisQPrefix+nm, di.conf.DisQPath,
				di.conf.DisQMaxFileSize, di.conf.DisQMinMsgSize, di.conf.DisQMaxMsgSize,
				di.conf.DisQSyncEvery, di.conf.DisQSyncTimeout,
				func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
					if diskQLogger != nil && lvl >= diskqueue.INFO {
						diskQLogger.Info(fmt.Sprintf(f, args...))
					}
				})
			if q.Depth() == 0 {
				if err := q.Empty(); err != nil {
					di.mu.Unlock()
					return fmt.Errorf("empty %q: %w", nm, err)
				}
			}
			di.diskQs[nm] = q
		}
		di.mu.Unlock()
	}

	one := func() error {
		var qCh <-chan []byte
		if q != nil {
			qCh = q.ReadChan()
		}
		var task Task
		var ok bool
		var source string
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok = <-inCh:
			if !ok {
				return fmt.Errorf("%q: %w", nm, errChannelClosed)
			}
			source = "inCh[" + nm + "]"
			// fast path
		case b, ok := <-qCh:
			if !ok {
				return nil
			}
			if err := task.UnmarshalProtobuf(b); err != nil {
				di.conf.Error("unmarshal", "bytes", b, "error", err)
				return errContinue
			}
			source = "qCh[" + nm + "]"
		}
		di.conf.Info("consume", "task", task, "source", source)
		if task.IsZero() {
			return errContinue
		}

		logger := di.conf.With(slog.String("name", task.Name), slog.String("refID", task.RefID))
		if task.Name == "" {
			logger.Info("empty task", "task", task, "source", source)
			return errContinue
		}
		deadline := task.Deadline
		if deadline.IsZero() {
			deadline = time.Now().Add(di.conf.Timeout)
		}
		if !deadline.After(time.Now()) {
			logger.Info("skip overtime", slog.Time("deadline", deadline), slog.String("refID", task.RefID))
			if task.RefID != "" {
				_ = di.answer(task.RefID, nil, nil, context.DeadlineExceeded)
			}
			// taskPool.Release(task)
			return nil
		}

		logger.Info("begin", slog.Time("deadline", deadline),
			slog.Int("payloadLen", len(task.Payload)),
			slog.Int("blobsCount", len(task.Blobs)))
		select {
		case <-ctx.Done():
			di.conf.Warn("context done", "error", ctx.Err())
			return ctx.Err()
		case gate <- struct{}{}:
			// Execute on a separate goroutine
			go func() {
				defer func() { <-gate }() //; taskPool.Release(task) }()
				logger.Debug("execute")
				di.execute(ctx, task)
				logger.Info("end", slog.Time("deadline", deadline),
					slog.String("source", source))
			}()
		}
		return nil
	}

	for {
		if err := one(); err == nil ||
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
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

	logger.Info("parse",
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

	logger.Debug("execute", "payload", string(task.Payload))

	var res *bytes.Buffer
	select {
	case res = <-di.buffers:
		res.Reset()
	default:
		var x bytes.Buffer
		res = &x
	}
	defer func() {
		select {
		case di.buffers <- res:
			res.Reset()
		default:
		}
	}()
	if err := ctx.Err(); err != nil {
		return
	}
	start := time.Now()
	r, callErr := di.do(ctx, res, task)
	logger.Info("call", "dur", time.Since(start).String(), "error", callErr)
	if errors.Is(callErr, ErrExit) || di.putQ == nil {
		return
	}
	start = time.Now()
	err := di.answer(task.RefID, res.Bytes(), r, callErr)
	logger.Info("pack", "length", res.Len(), "dur", time.Since(start).String(), "error", err)
}

// answer puts the answer into the queue.
func (di *Dispatcher) answer(refID string, payload []byte, blob io.Reader, err error) error {
	di.conf.Debug("answer", "refID", refID, "payload", payload, "error", err)
	if di.putQ == nil {
		return nil
	}
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
		if obj, err = di.putQ.PayloadObjectType.NewObject(); err != nil {
			return err
		}
	}
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
	select {
	case di.putObjects <- obj:
		_ = obj.ResetAttributes()
	default:
		_ = obj.Close()
	}
	return nil
}

func dbCtx(ctx context.Context, module, action string) context.Context {
	return godror.ContextWithTraceTag(ctx, godror.TraceTag{Module: module, Action: action})
}
