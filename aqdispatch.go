// Copyright 2021 Tamás Gulácsi. All rights reserved.
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
	"io/ioutil"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/UNO-SOFT/aqdispatch/pb"
	"github.com/go-kit/kit/log"
	"github.com/godror/godror"
	"github.com/nsqio/go-diskqueue"
	"golang.org/x/text/encoding"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:generate go get google.golang.org/protobuf/cmd/protoc-gen-go
//go:generate protoc --proto_path=pb --go_out=pb -I../../../ --go_opt=paths=source_relative pb/task.proto

type Config struct {
	log.Logger
	Debug log.Logger
	//Tracer:     tracer,

	Enc encoding.Encoding

	DisQPrefix, DisQPath           string
	DisQMaxFileSize, DisQSyncEvery int64
	DisQMinMsgSize, DisQMaxMsgSize int32
	DisQSyncTimeout                time.Duration

	AnswerKeyErrMsg, AnswerKeyPayload string

	Timeout, PipeTimeout time.Duration
	FuncCount            int
	Concurrency          int
}

type Task = pb.Task

func New(
	ctx context.Context, db *sql.DB,
	conf Config,
	inQName, inQType string,
	do func(context.Context, io.Writer, Task) error,
	outQName, outQType string,
) (*Dispatcher, error) {
	if conf.AnswerKeyErrMsg == "" {
		conf.AnswerKeyErrMsg = "HIBASZOV"
	}
	if conf.AnswerKeyPayload == "" {
		conf.AnswerKeyPayload = "PAYLOAD"
	}
	if conf.DisQPrefix == "" {
		conf.DisQPrefix = "aqdispatch-"
	}
	if conf.DisQPath == "" {
		conf.DisQPath = "."
	}
	os.MkdirAll(conf.DisQPath, 0750)
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
		conf.DisQSyncEvery = 1
	}
	if conf.DisQSyncTimeout <= 0 {
		conf.DisQSyncTimeout = time.Second
	}

	if conf.Concurrency <= 0 {
		conf.Concurrency = runtime.GOMAXPROCS(-1)
	}
	if conf.Logger == nil {
		conf.Logger = log.NewNopLogger()
	}
	if do == nil {
		return nil, errors.New("do function cannot be nil")
	}
	if inQName == "" || outQName == "" || inQType == "" || outQType == "" {
		return nil, errors.New("inQName, inQType and outQName, outQType are required")
	}
	di := Dispatcher{
		conf:       conf,
		do:         do,
		ins:        make(map[string]chan Task, conf.FuncCount),
		diskQs:     make(map[string]diskqueue.Interface, conf.FuncCount),
		gates:      make(map[string]chan struct{}, conf.FuncCount),
		putObjects: make(chan *godror.Object, conf.Concurrency),
		datas:      make(chan *godror.Data, conf.Concurrency),
		buffers:    make(chan *bytes.Buffer, conf.Concurrency),
		deqMsgs:    make([]godror.Message, conf.Concurrency),
	}

	cx, err := db.Conn(dbCtx(ctx, "aqdispatch", inQName))
	if err != nil {
		return nil, err
	}
	defer cx.Close()

	if err = godror.Raw(ctx, cx, func(conn godror.Conn) error {
		di.Timezone = conn.Timezone()
		return nil
	}); err != nil {
		return nil, err
	}
	if di.getQ, err = godror.NewQueue(ctx, cx, inQName, inQType); err != nil {
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

	cx, err = db.Conn(dbCtx(ctx, "aqdispatch", outQName))
	if err != nil {
		di.Close()
		return nil, err
	}
	defer cx.Close()
	if di.putQ, err = godror.NewQueue(ctx, cx, outQName, outQType); err != nil {
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

// Dispatcher. After creating with New, start a Consumer for each task Func name!
type Dispatcher struct {
	conf Config
	do   func(context.Context, io.Writer, Task) error
	//db         *sql.DB
	mu         sync.RWMutex
	getQ       *godror.Queue
	putQ       *godror.Queue
	ins        map[string]chan Task
	diskQs     map[string]diskqueue.Interface
	gates      map[string]chan struct{}
	datas      chan *godror.Data
	putObjects chan *godror.Object
	buffers    chan *bytes.Buffer
	Timezone   *time.Location
	deqMsgs    []godror.Message
}

func (di *Dispatcher) Log(keyvals ...interface{}) error {
	return di.conf.Logger.Log(keyvals...)
}

func (di *Dispatcher) Decode(p []byte) string {
	if di.conf.Enc == nil {
		return string(p)
	}
	q, _ := di.conf.Enc.NewDecoder().Bytes(p)
	return string(q)
}
func (di *Dispatcher) Encode(s string) string {
	if di.conf.Enc == nil {
		return s
	}
	s, _ = encoding.ReplaceUnsupported(di.conf.Enc.NewEncoder()).String(s)
	return s
}

func (di *Dispatcher) Close() error {
	ins, putO, diskQs := di.ins, di.putObjects, di.diskQs
	di.ins, di.putObjects, di.diskQs = nil, nil, nil
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
	return nil
}

var (
	ErrUnknownCommand = errors.New("unknown command")
	ErrSkipResponse   = errors.New("skip response")
	ErrEmpty          = errors.New("empty")
	ErrExit           = errors.New("exit")
)

func (di *Dispatcher) Batch(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	n, err := di.getQ.Dequeue(di.deqMsgs[:])
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
	ctx, cancel := context.WithTimeout(ctx, di.conf.Timeout)
	defer cancel()
	for i := range di.deqMsgs[:n] {
		// The messages are tightly coupled with the queue,
		// so we must parse them sequentially.
		task, err := di.parse(ctx, &di.deqMsgs[i])
		if err != nil {
			if errors.Is(err, ErrEmpty) {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		di.mu.RLock()
		inCh, ok := di.ins[task.Func]
		di.mu.RUnlock()
		if !ok {
			if firstErr == nil {
				firstErr = ErrUnknownCommand
			}
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case inCh <- task:
		// Skip the disk queue
		default:
			// diskQs are for traffic jams
			b, err := proto.Marshal(&task)
			if err != nil {
				return err
			}
			di.Log("msg", "enqueue", "task", task.Func, "deadline", task.Deadline)
			di.mu.RLock()
			q, ok := di.diskQs[task.Func]
			di.mu.RUnlock()
			if !ok {
				if firstErr == nil {
					firstErr = ErrUnknownCommand
				}
				continue
			}
			if err = q.Put(b); err != nil {
				di.Log("msg", "enqueue", "queue", task.Func, "error", err)
				return fmt.Errorf("put slurpus task into %q queue: %w", task.Func, err)
			}
		}
	}
	return firstErr
}

// Consume the named queue. Does not return.
//
// MUST be started for each function name!
func (di *Dispatcher) Consume(ctx context.Context, nm string) error {
	di.mu.RLock()
	inCh, gate, q := di.ins[nm], di.gates[nm], di.diskQs[nm]
	di.mu.RUnlock()
	if inCh == nil || gate == nil || q == nil {
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

		if q == nil {
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
					return fmt.Errorf("empty %q: %w", nm, err)
				}
			}
			di.diskQs[nm] = q
		}
		di.mu.Unlock()
	}

	var token struct{}
Loop:
	for {
		var task Task
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task = <-inCh:
			// fast path
		case b := <-q.ReadChan():
			if err := proto.Unmarshal(b, &task); err != nil {
				di.Log("msg", "unmarshal", "bytes", b, "error", err)
				continue Loop
			}
		}

		if !task.GetDeadline().AsTime().After(time.Now()) {
			di.Log("msg", "skip overtime", "deadline", task.Deadline)
			_ = di.answer(task.RefID, nil, context.DeadlineExceeded)
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case gate <- token:
			// Execution can go on a separate goroutine.
			go func() {
				defer func() { <-gate }()
				di.Log("msg", "execute queued", "task", task.Func, "deadline", task.Deadline)
				di.execute(ctx, task)
			}()
		}
	}
	return nil
}

func (di *Dispatcher) parse(ctx context.Context, msg *godror.Message) (task Task, err error) {
	if msg != nil && msg.Object != nil {
		defer func() {
			if msg != nil && msg.Object != nil {
				msg.Object.Close()
			}
		}()
	}
	select {
	case <-ctx.Done():
		return task, ctx.Err()
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
	task.Deadline = timestamppb.New(msg.Deadline())
	if task.Deadline.IsValid() {
		task.Deadline = timestamppb.New(task.Deadline.AsTime().Add(-1 * time.Second))
	} else {
		task.Deadline = timestamppb.New(time.Now().Add(di.conf.Timeout))
	}
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
	if err = obj.GetAttribute(data, "URL"); err != nil {
		return task, fmt.Errorf("get URL: %w", err)
	}
	task.URL = string(data.GetBytes())
	if debug != nil {
		debug.Log("msg", "get url", "data", data, "url", task.URL)
	}
	if err = obj.GetAttribute(data, "FUNC"); err != nil {
		return task, fmt.Errorf("get FUNC: %w", err)
	}
	task.Func = string(data.GetBytes())
	if debug != nil {
		debug.Log("msg", "get func", "data", data, "func", task.Func)
	}

	if err = obj.GetAttribute(data, "PAYLOAD"); err != nil {
		return task, fmt.Errorf("get PAYLOAD: %w", err)
	}
	if debug != nil {
		debug.Log("msg", "get payload", "data", data)
	}
	task.Payload, err = ioutil.ReadAll(data.GetLob())
	if err != nil {
		return task, fmt.Errorf("getLOB: %w", err)
	}
	logger.Log("msg", "parse", "url", task.URL, "func", task.Func, "payloadLen", len(task.Payload), "enqueued", msg.Enqueued, "delay", msg.Delay, "expiry", msg.Expiration, "deadline", task.Deadline)
	if task.RefID == "" || task.URL == "" || task.Func == "" {
		return task, ErrEmpty
	}

	return task, nil
}

func (di *Dispatcher) execute(ctx context.Context, task Task) error {
	if task.RefID == "" || task.URL == "" || task.Func == "" {
		return ErrEmpty
	}

	logger := log.With(di.conf.Logger, "refID", task.RefID)
	debug := di.conf.Debug
	if debug != nil {
		debug = log.With(debug, "refID", task.RefID)
	}
	if task.Deadline.IsValid() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, task.Deadline.AsTime())
		defer cancel()
	}
	logger.Log("msg", "execute", "func", task.Func, "url", task.URL, "payloadLen", len(task.Payload), "deadline", task.Deadline.AsTime().In(time.Local))
	if debug != nil {
		debug.Log("msg", "execute", "payload", string(task.Payload))
	}
	/*
		if di.Tracer != nil {
			tCtx, span := di.Tracer.Start(ctx, "task="+task.Func)
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
		return err
	}
	start := time.Now()
	callErr := di.do(ctx, res, task)
	logger.Log("msg", "call", "dur", time.Since(start), "error", callErr)
	if callErr == ErrExit {
		return callErr
	}
	start = time.Now()
	err := di.answer(task.RefID, res.Bytes(), callErr)
	logger.Log("msg", "pack", "length", res.Len(), "dur", time.Since(start), "error", err)
	return err
}

func (di *Dispatcher) answer(refID string, payload []byte, err error) error {
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
		if err = obj.Set(di.conf.AnswerKeyErrMsg, errMsg); err != nil {
			obj.Close()
			return fmt.Errorf("set %s: %w", di.conf.AnswerKeyErrMsg, err)
		}
	}
	if len(payload) != 0 {
		if err = obj.Set(di.conf.AnswerKeyPayload, payload); err != nil {
			obj.Close()
			return fmt.Errorf("set %s: %w", di.conf.AnswerKeyPayload, err)
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
		obj.ResetAttributes()
	default:
		obj.Close()
	}
	return nil
}

func dbCtx(ctx context.Context, module, action string) context.Context {
	return godror.ContextWithTraceTag(ctx, godror.TraceTag{Module: module, Action: action})
}
