// Copyright 2021 Tamás Guácsi. All rights reserved.
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
	"os"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/go-kit/kit/log"
	"github.com/godror/godror"
	"github.com/nsqio/go-diskqueue"

	"github.com/UNO-SOFT/aqdispatch/pb"
)

//go:generate go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
//go:generate protoc --proto_path=pb --go_out=pb -I../../../ --go_opt=paths=source_relative pb/task.proto
//go:generate sed -i -e "/\"github.com\\/golang\\/protobuf\\/ptypes\\/timestamp\"/ s,\".*\",\"google.golang.org/protobuf/types/known/timestamppb\"," pb/task.pb.go

// Config of the Dispatcher.
type Config struct {
	log.Logger
	Debug log.Logger
	//Tracer:     tracer,

	// Enc is needed when DB sends/requires something other than UTF-8
	Enc encoding.Encoding

	// DisQPrefix is the diskqueue file's prefix.
	DisQPrefix string
	// DisQPath is the path for the diskqueue.
	DisQPath                       string
	DisQMaxFileSize, DisQSyncEvery int64
	DisQMinMsgSize, DisQMaxMsgSize int32
	DisQSyncTimeout                time.Duration

	// RequestKeyName is the attribute name instead of NAME.
	RequestKeyName string
	// RequestKeyPayload is the attribute name instead of PAYLOAD
	RequestKeyPayload string
	// ResponseKeyErrMsg is the attribute name instead of ERRMSG
	ResponseKeyErrMsg string
	// ResponseKeyPayload is the attribute name instead of PAYLOAD
	ResponseKeyPayload string

	Timeout, PipeTimeout time.Duration
	// QueueCount is the approximate number of queues dispatched over this AQ.
	QueueCount int
	// Concurrency is the number of concurrent RPCs.
	Concurrency int
}

// Task is a task.
type Task = pb.Task

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
	do func(context.Context, io.Writer, *Task) error,
	outQName, outQType string,
) (*Dispatcher, error) {
	if conf.RequestKeyName == "" {
		conf.RequestKeyName = "NAME"
	}
	if conf.RequestKeyPayload == "" {
		conf.RequestKeyPayload = "PAYLOAD"
	}
	if conf.ResponseKeyErrMsg == "" {
		conf.ResponseKeyErrMsg = "ERRMSG"
	}
	if conf.ResponseKeyPayload == "" {
		conf.ResponseKeyPayload = "PAYLOAD"
	}
	if conf.DisQPrefix == "" {
		conf.DisQPrefix = "aqdispatch-"
	}
	if conf.DisQPath == "" {
		conf.DisQPath = "."
	}
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
	if conf.Logger == nil {
		conf.Logger = log.NewNopLogger()
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
		ins:     make(map[string]chan *Task, conf.QueueCount),
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
	if di.getQ, err = godror.NewQueue(ctx, getCx, inQName, inQType); err != nil {
		di.Close()
		return nil, err
	}
	di.Log("msg", "getQ", "name", di.getQ.Name())
	dOpts, err := di.getQ.DeqOptions()
	if err != nil {
		di.Close()
		return nil, err
	}
	dOpts.Mode = godror.DeqRemove
	dOpts.Navigation = godror.NavFirst
	dOpts.Visibility = godror.VisibleImmediate
	dOpts.Wait = conf.PipeTimeout
	if err = di.getQ.SetDeqOptions(dOpts); err != nil {
		di.Close()
		return nil, err
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
	if di.putQ, err = godror.NewQueue(ctx, putCx, outQName, outQType); err != nil {
		di.Close()
		return nil, err
	}
	di.Log("msg", "putQ", "name", di.putQ.Name())
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
	return &di, nil
}

// Run the dispatcher, accepting tasks with names in taskNames.
func (di *Dispatcher) Run(ctx context.Context, taskNames []string) error {
	grp, ctx := errgroup.WithContext(ctx)
	for _, nm := range taskNames {
		nm := nm
		grp.Go(func() error { return di.consume(ctx, nm, len(taskNames) <= 1) })
	}
	if di.conf.Debug != nil {
		di.conf.Debug.Log("msg", "prepared consumers", "taskNames", taskNames)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := di.batch(ctx); err != nil {
			di.Log("msg", "batch finished", "error", err)
			var ec interface{ Code() int }
			if godror.IsBadConn(err) || errors.As(err, &ec) && ec.Code() == 24010 { // ORA-24010: Queue does not exist
				return err
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
	conf         Config
	do           func(context.Context, io.Writer, *Task) error
	getCx, putCx io.Closer
	mu           sync.RWMutex
	getQ         *godror.Queue
	putQ         *godror.Queue
	ins          map[string]chan *Task
	diskQs       map[string]diskqueue.Interface
	gates        map[string]chan struct{}
	datas        chan *godror.Data
	putObjects   chan *godror.Object
	buffers      chan *bytes.Buffer
	Timezone     *time.Location
	deqMsgs      []godror.Message
}

func (di *Dispatcher) Log(keyvals ...interface{}) {
	_ = di.conf.Logger.Log(keyvals...)
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

	errChannelClosed = errors.New("channel is closed")
	errContinue      = errors.New("continue")
)
var taskPool = tskPool{pool: &sync.Pool{New: func() interface{} { var t Task; return &t }}}

// batch process at most Config.Concurrency number of messages: wait at max Config.Timeout,
// then for each message, decode it, send to the named channel if possible,
// otherwise save it to a specific diskqueue.
func (di *Dispatcher) batch(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	n, err := di.getQ.Dequeue(di.deqMsgs[:])
	if di.conf.Debug != nil {
		di.conf.Debug.Log("msg", "dequeue", "n", n, "error", err)
	}
	if n > 0 || err != nil {
		di.Log("msg", "dequeue", "n", n, "error", err)
	}
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	var firstErr error
	one := func(ctx context.Context, task *Task, msg *godror.Message) error {
		// The messages are tightly coupled with the queue,
		// so we must parse them sequentially.
		err = di.parse(ctx, task, msg)
		if err != nil {
			if errors.Is(err, ErrEmpty) {
				return errContinue
			}
			if firstErr == nil {
				firstErr = err
			}
			return errContinue
		}

		nm := task.Name
		di.mu.RLock()
		inCh, ok := di.ins[nm]
		if !ok {
			if di.conf.Debug != nil {
				di.conf.Debug.Log("msg", "unknown task", "name", nm)
			}
			if inCh, ok = di.ins[""]; ok {
				nm = ""
			}
		}
		q := di.diskQs[nm]
		di.mu.RUnlock()
		if !ok {
			if firstErr == nil {
				firstErr = ErrUnknownCommand
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
			b, err := proto.Marshal(task)
			if err != nil {
				return err
			}
			di.Log("msg", "enqueue", "task", task.Name, "deadline", task.GetDeadline().AsTime())
			if err = q.Put(b); err != nil {
				di.Log("msg", "enqueue", "queue", task.Name, "error", err)
				return fmt.Errorf("put surplus task into %q queue: %w", task.Name, err)
			}
			return errContinue // release Task
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, di.conf.Timeout)
	defer cancel()
	for i := range di.deqMsgs[:n] {
		task := taskPool.Acquire()
		if err := one(ctx, task, &di.deqMsgs[i]); err != nil {
			taskPool.Release(task)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			if !errors.Is(err, errContinue) {
				return err
			}
		}
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
	if di.conf.Debug != nil {
		di.conf.Debug.Log("msg", "consume", "name", nm)
	}
	di.mu.RLock()
	inCh, gate, q := di.ins[nm], di.gates[nm], di.diskQs[nm]
	di.mu.RUnlock()
	if inCh == nil || gate == nil || q == nil && !noDisQ {
		di.mu.Lock()
		inCh, gate, q = di.ins[nm], di.gates[nm], di.diskQs[nm]
		if inCh == nil {
			inCh = make(chan *Task)
			di.ins[nm] = inCh
		}
		if gate == nil {
			gate = make(chan struct{}, di.conf.Concurrency)
			di.gates[nm] = gate
		}
		if q == nil && !noDisQ {
			diskQLogger := log.With(di.conf.Logger, "lib", "diskqueue", "nm", nm)
			q = diskqueue.New(di.conf.DisQPrefix+nm, di.conf.DisQPath,
				di.conf.DisQMaxFileSize, di.conf.DisQMinMsgSize, di.conf.DisQMaxMsgSize,
				di.conf.DisQSyncEvery, di.conf.DisQSyncTimeout,
				func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
					if lvl >= diskqueue.INFO {
						diskQLogger.Log("msg", fmt.Sprintf(f, args...))
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
		var task *Task
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok = <-inCh:
			if !ok {
				return fmt.Errorf("%q: %w", nm, errChannelClosed)
			}
			// fast path
		case b, ok := <-qCh:
			if !ok {
				return nil
			}
			task = taskPool.Acquire()
			if err := proto.Unmarshal(b, task); err != nil {
				di.Log("msg", "unmarshal", "bytes", b, "error", err)
				return errContinue
			}
		}
		if di.conf.Debug != nil {
			di.conf.Debug.Log("msg", "consume", "task", task)
		}
		if task == nil {
			return errContinue
		}
		if task.Name == "" {
			di.Log("msg", "empty task", "task", task)
			taskPool.Release(task)
			return errContinue
		}
		var deadline time.Time
		if dl := task.GetDeadline(); dl.IsValid() {
			deadline = dl.AsTime()
		} else {
			deadline = time.Now().Add(di.conf.Timeout)
		}
		if !deadline.After(time.Now()) {
			di.Log("msg", "skip overtime", "deadline", deadline, "refID", task.RefID)
			if task.RefID != "" {
				_ = di.answer(task.RefID, nil, context.DeadlineExceeded)
			}
			taskPool.Release(task)
			return nil
		}
		name := task.Name
		di.Log("msg", "begin", "task", name, "deadline", deadline, "payloadLen", len(task.Payload))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case gate <- struct{}{}:
			// Execute on a separate goroutine
			go func() {
				defer func() { <-gate; taskPool.Release(task) }()
				di.execute(ctx, task)
				di.Log("msg", "end", "task", name, "deadline", deadline)
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
	task.Deadline = timestamppb.New(deadline)
	if task.RefID == "" {
		task.RefID = fmt.Sprintf("%X", msg.MsgID[:])
	}
	logger := log.With(di.conf.Logger, "refID", task.RefID)
	debug := di.conf.Debug
	if debug != nil {
		debug = log.With(debug, "refID", task.RefID)
	}
	obj := msg.Object
	if debug != nil {
		debug.Log("msg", msg, "obj", obj)
	}
	if err := obj.GetAttribute(data, di.conf.RequestKeyName); err != nil {
		return fmt.Errorf("get %s: %w", di.conf.RequestKeyName, err)
	}
	task.Name = string(data.GetBytes())
	if debug != nil {
		debug.Log("msg", "get task name", "data", data, "name", task.Name)
	}

	if err := obj.GetAttribute(data, di.conf.RequestKeyPayload); err != nil {
		return fmt.Errorf("get %s: %w", di.conf.RequestKeyPayload, err)
	}
	if debug != nil {
		debug.Log("msg", "get payload", "data", data)
	}
	var buf bytes.Buffer
	godror.Log = logger.Log
	if n, err := io.Copy(&buf, data.GetLob()); err != nil {
		return fmt.Errorf("getLOB: %w", err)
	} else {
		logger.Log("msg", "LOB read", "n", n, "length", buf.Len())
	}
	godror.Log = nil
	task.Payload = buf.Bytes()
	logger.Log("msg", "parse", "name", task.Name, "payloadLen", len(task.Payload),
		"enqueued", msg.Enqueued, "delay", msg.Delay.String(), "expiry", msg.Expiration.String(),
		"deadline", deadline)
	if task.RefID == "" || task.Name == "" {
		return ErrEmpty
	}

	return nil
}

// execute calls do on the task, then calls answer with the answer.
func (di *Dispatcher) execute(ctx context.Context, task *Task) {
	if di.conf.Debug != nil {
		di.conf.Debug.Log("msg", "execute", "task", task)
	}
	if task.RefID == "" || task.Name == "" {
		di.Log("msg", "execute skip empty task", "task", task)
		return
	}

	logger := log.With(di.conf.Logger, "refID", task.RefID)
	debug := di.conf.Debug
	if debug != nil {
		debug = log.With(debug, "refID", task.RefID)
	}
	var deadline time.Time
	if dl := task.GetDeadline(); dl.IsValid() {
		deadline = dl.AsTime()
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	logger.Log("msg", "execute", "name", task.Name, "payloadLen", len(task.Payload), "deadline", deadline)
	if debug != nil {
		debug.Log("msg", "execute", "payload", string(task.Payload))
	}
	/*
		if di.Tracer != nil {
			tCtx, span := di.Tracer.Start(ctx, "task="+task.Name)
			ctx = tCtx
			defer span.End()
		}
	*/

	if debug != nil {
		debug.Log("msg", "execute", "payload", string(task.Payload))
	}

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
	callErr := di.do(ctx, res, task)
	logger.Log("msg", "call", "dur", time.Since(start).String(), "error", callErr)
	if errors.Is(callErr, ErrExit) || di.putQ == nil {
		return
	}
	start = time.Now()
	err := di.answer(task.RefID, res.Bytes(), callErr)
	logger.Log("msg", "pack", "length", res.Len(), "dur", time.Since(start).String(), "error", err)
}

// answer puts the answer into the queue.
func (di *Dispatcher) answer(refID string, payload []byte, err error) error {
	if di.conf.Debug != nil {
		di.conf.Debug.Log("msg", "answer", "refID", refID, "payload", payload, "error", err)
	}
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
	logger := log.With(di.conf.Logger, "refID", refID)
	logger.Log("msg", "answer", "errMsg", errMsg)
	if di.conf.Debug != nil {
		di.conf.Debug.Log("msg", "answer", "payload", string(payload))
	}

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
		if di.conf.Debug != nil {
			di.conf.Debug.Log("msg", "create", "object", di.putQ.PayloadObjectType)
		}
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
