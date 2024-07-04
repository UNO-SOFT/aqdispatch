// Copyright 2021, 2024 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package aqdispatch

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding"

	"github.com/godror/godror"
)

// Config of the Dispatcher.
type Config struct {
	// Enc is needed when DB sends/requires something other than UTF-8
	Enc encoding.Encoding

	*slog.Logger
	//Tracer:     tracer,

	// RequestKeyName is the attribute name instead of NAME.
	RequestKeyName string

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

	Timeout, PipeTimeout time.Duration
	// Concurrency is the number of concurrent RPCs.
	Concurrency int

	DisQMinMsgSize, DisQMaxMsgSize int32
}

// DoFunc is the type of the function that processes the Task.
type DoFunc func(context.Context, io.Writer, *Task) (io.Reader, error)

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
	taskNames []string,
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
	// nosemgrep: go.lang.correctness.permissions.file_permission.incorrect-default-permission

	if conf.Timeout <= 0 {
		conf.Timeout = 30 * time.Second
	}
	if conf.PipeTimeout <= 0 {
		conf.PipeTimeout = 30 * time.Second
	}
	if conf.Concurrency <= 0 {
		conf.Concurrency = runtime.GOMAXPROCS(-1)
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
	di := Dispatcher{
		conf:    conf,
		do:      do,
		ins:     make(map[string]chan *Task, len(taskNames)),
		gates:   make(map[string]chan struct{}, len(taskNames)),
		getQs:   make(map[string]queue, len(taskNames)),
		datas:   make(chan *godror.Data, conf.Concurrency),
		buffers: make(chan *bytes.Buffer, conf.Concurrency),
		deqMsgs: make(map[string][]godror.Message, len(taskNames))}

	ctx := context.Background()
	if err := func() error {
		cx, err := db.Conn(dbCtx(ctx, "aqdispatch", inQName))
		if err != nil {
			return err
		}
		defer cx.Close()

		if err = godror.Raw(ctx, cx, func(conn godror.Conn) error {
			di.Timezone = conn.Timezone()
			return nil
		}); err != nil {
			return err
		}

		getQ, err := godror.NewQueue(ctx, cx, inQName, inQType)
		if err != nil {
			di.Close()
			return err
		}
		defer getQ.Close()
		di.conf.Info("getQ", "name", getQ.Name())
		if err = getQ.PurgeExpired(ctx); err != nil {
			di.conf.Warn("PurgeExpired", "queue", getQ.Name(), "error", err)
		}
		_, di.getQHasBlob = getQ.PayloadObjectType.Attributes[di.conf.RequestKeyBlob]
		return nil
	}(); err != nil {
		di.Close()
		return nil, err
	}

	for _, nm := range taskNames {
		var getQ *godror.Queue
		var cx *sql.Conn
		if err := func() error {
			var err error
			if cx, err = db.Conn(dbCtx(ctx, "aqdispatch-"+nm, inQName)); err != nil {
				return err
			}
			getQ, err = godror.NewQueue(ctx, cx, inQName, inQType)
			if err != nil {
				return err
			}
			di.conf.Info("getQ", "name", getQ.Name())
			dOpts, err := getQ.DeqOptions()
			if err != nil {
				return err
			}
			dOpts.Mode = godror.DeqRemove
			dOpts.Navigation = godror.NavFirst
			dOpts.Visibility = godror.VisibleImmediate
			dOpts.Wait = di.conf.PipeTimeout
			// dOpts.Wait = 100 * time.Millisecond
			// dOpts.Correlation = di.conf.Correlation
			dOpts.Condition = "tab.user_data." + di.conf.RequestKeyName +
				"=UTL_i18n.raw_to_char(UTL_ENCODE.BASE64_DECODE(UTL_RAW.cast_to_raw('" +
				base64.StdEncoding.EncodeToString([]byte(nm)) +
				"')), 'AL32UTF8')"
			// dOpts.Consumer MUST NOT be set (ORA-24039).
			di.conf.Debug("SetDeqOptions", "queue", nm, "options", dOpts)
			return getQ.SetDeqOptions(dOpts)
		}(); err != nil {
			if getQ != nil {
				getQ.Close()
			}
			if cx != nil {
				cx.Close()
			}
			di.Close()
			return nil, err
		}
		di.getQs[nm] = queue{cx: cx, Queue: getQ}
		di.deqMsgs[nm] = make([]godror.Message, conf.Concurrency)
		di.ins[nm] = make(chan *Task)
	}

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
	for _, q := range di.getQs {
		if q.Queue != nil {
			if err := q.PurgeExpired(ctx); err != nil && firstErr != nil {
				firstErr = err
			}
		}
	}
	if di.putQ != nil {
		if err := di.putQ.PurgeExpired(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Run the dispatcher.
func (di *Dispatcher) Run(ctx context.Context) error {
	grp, ctx := errgroup.WithContext(ctx)
	for nm := range di.getQs {
		nm := nm
		grp.Go(func() error { return di.consume(ctx, nm) })
	}
	timer := time.NewTimer(di.conf.PipeTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := di.batch(ctx); err != nil {
			if err == io.EOF {
				continue
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

type queue struct {
	*godror.Queue
	cx *sql.Conn
}

func (q queue) Close() error {
	if q.Queue != nil {
		q.Queue.Close()
	}
	if q.cx != nil {
		q.cx.Close()
	}
	return nil
}

// Dispatcher. After creating with New, run it with Run.
//
// Reads tasks and store the messages in diskqueues - one for each distinct NAME.
// If Concurrency allows, calls the do function given in New,
// and sends the answer as PAYLOAD of what that writes as response.
type Dispatcher struct {
	putCx                    io.Closer
	putConn                  godror.Conn
	deqMsgs                  map[string][]godror.Message
	gates                    map[string]chan struct{}
	putObjects               chan *godror.Object
	ins                      map[string]chan *Task
	getQs                    map[string]queue
	putQ                     *godror.Queue
	datas                    chan *godror.Data
	do                       DoFunc
	buffers                  chan *bytes.Buffer
	Timezone                 *time.Location
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
	ins, putO, getQs, putQ := di.ins, di.putObjects, di.getQs, di.putQ
	di.ins, di.putObjects, di.getQs, di.putQ = nil, nil, nil, nil
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
	for _, q := range getQs {
		q.Close()
	}
	if putQ != nil {
		putQ.Close()
	}
	putCx := di.putCx
	di.putCx = nil
	if putCx != nil {
		putCx.Close()
	}
	return nil
}

var (
	ErrUnknownCommand = errors.New("unknown command")
	ErrSkipResponse   = errors.New("skip response")
	ErrEmpty          = errors.New("empty")
	ErrExit           = errors.New("exit")
	ErrAnswer         = errors.New("answer send error")

	errChannelClosed = errors.New("channel is closed")
	errContinue      = errors.New("continue")

	taskPool = tskPool{pool: &sync.Pool{New: func() interface{} { var t Task; return &t }}}
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

	var firstErr error
	one := func(ctx context.Context, nm string, inCh chan<- *Task, msg *godror.Message) error {
		// The messages are tightly coupled with the queue,
		// so we must parse them sequentially.
		task := taskPool.Acquire()
		var keepTask bool
		func() {
			if !keepTask {
				taskPool.Release(task)
			}
		}()
		err := di.parse(ctx, task, msg)
		if err != nil {
			di.conf.Warn("parse", "error", err)
			if errors.Is(err, ErrEmpty) {
				return errContinue
			}
			if firstErr == nil {
				firstErr = err
			}
			return errContinue
		}
		if nm != task.Name {
			return fmt.Errorf("%w: wanted %q, got %q",
				ErrUnknownCommand, nm, task.Name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inCh <- task:
			keepTask = true
			return nil
		}
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.SetLimit(di.conf.Concurrency)
	for nm := range di.getQs {
		nm := nm
		grp.Go(func() error {
			// if err := func() error {
			logger := di.conf.Logger.With(slog.String("queue", nm))
			Q, msgs := di.getQs[nm], di.deqMsgs[nm]
			logger.Debug("start dequeue", slog.Int("msgs", len(msgs)))
			n, err := Q.Dequeue(msgs)
			if err != nil {
				logger.Error("dequeue", "error", err)
				return err
			}
			logger.Info("dequeue", "n", n)
			if n == 0 {
				return nil
			}
			ctx, cancel := context.WithTimeout(grpCtx, di.conf.Timeout)
			defer cancel()
			inCh := di.ins[nm]
			for i := range msgs[:n] {
				if err := one(ctx, nm, inCh, &msgs[i]); err != nil {
					logger.Error("one", "error", err)
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return nil
					}
					if !errors.Is(err, errContinue) {
						return err
					}
				}
			}
			return nil
			// }(); err != nil && firstErr == nil {
			// firstErr = err
			// }
		})
	}
	if err := grp.Wait(); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr != nil {
		di.conf.Error("batch", "error", firstErr)
	}
	return firstErr
}

// consume the named queue. Does not return.
//
// MUST be started for each function name!
//
// When nm is the empty string, that's a catch-all (not catched by others).
func (di *Dispatcher) consume(ctx context.Context, nm string) error {
	di.conf.Debug("consume", "name", nm)
	di.mu.RLock()
	inCh, gate := di.ins[nm], di.gates[nm]
	di.mu.RUnlock()
	if gate == nil {
		di.mu.Lock()
		if gate = di.gates[nm]; gate == nil {
			gate = make(chan struct{}, di.conf.Concurrency)
			di.gates[nm] = gate
		}
		di.mu.Unlock()
	}

	one := func() error {
		var task *Task
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok = <-inCh:
			if !ok {
				return fmt.Errorf("%q: %w", nm, errChannelClosed)
			}
		}
		di.conf.Debug("consume", "task", task)
		if task == nil {
			return errContinue
		}
		if task.Name == "" {
			di.conf.Info("empty task", "task", task)
			taskPool.Release(task)
			return errContinue
		}
		deadline := task.Deadline
		if deadline.IsZero() {
			deadline = time.Now().Add(di.conf.Timeout)
		}
		if !deadline.After(time.Now()) {
			di.conf.Info("skip overtime", "deadline", deadline, "refID", task.RefID)
			if task.RefID != "" {
				_ = di.answer(task.RefID, nil, nil, context.DeadlineExceeded)
			}
			taskPool.Release(task)
			return nil
		}
		name := task.Name
		di.conf.Info("begin", "task", name, "deadline", deadline, "payloadLen", len(task.Payload), "blobsCount", len(task.Blobs))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case gate <- struct{}{}:
			// Execute on a separate goroutine
			go func() {
				defer func() { <-gate; taskPool.Release(task) }()
				di.execute(ctx, task)
				di.conf.Info("end", "task", name, "deadline", deadline)
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

	logger.Info("parse", "name", task.Name, "payloadLen", len(task.Payload), "blobCount", len(task.Blobs),
		"enqueued", msg.Enqueued, "delay", msg.Delay.String(), "expiry", msg.Expiration.String(),
		"deadline", deadline)
	if task.RefID == "" || task.Name == "" {
		return ErrEmpty
	}

	return nil
}

// execute calls do on the task, then calls answer with the answer.
func (di *Dispatcher) execute(ctx context.Context, task *Task) {
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

type tskPool struct {
	pool *sync.Pool
}

func (p tskPool) Acquire() *Task { return p.pool.Get().(*Task) }
func (p tskPool) Release(t *Task) {
	if t != nil {
		*t = Task{}
		p.pool.Put(t)
	}
}
