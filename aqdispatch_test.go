// Copyright 2025 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package aqdispatch_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"golang.org/x/text/encoding"

	"github.com/UNO-SOFT/aqdispatch"
	"github.com/UNO-SOFT/zlog/v2"

	"github.com/godror/godror"
)

var (
	flagConnect = flag.String("connect", nvl(
		os.Getenv("BRUNO_ID"),
		"oracle://demo:demo@localhost:1521/freepdb1",
	), "database to connect to")
	flagSysConnect = flag.String("connect-sys", nvl(
		os.Getenv("BRUNO_OWNER_ID"),
		`system/system@localhost:1521/freepdb1 AS SYSDBA`,
	), "database to connect to as SYSDBA")
)

func TestAQ(t *testing.T) {
	var i int
	testAQ(t, nil, func() bool {
		if i >= 10 {
			return false
		}
		i++
		return true
	})
}
func BenchmarkAQ(b *testing.B) {
	testAQ(b, b.ResetTimer, b.Loop)
}

type payload struct{ ID int }

func testAQ(t testing.TB, resetTimer func(), loop func() bool) {
	srvReceived := make(map[int]chan struct{})
	var srvReceivedMu sync.Mutex
	ctx, closer, db, ex := setupAQ(t, func(ctx context.Context, w io.Writer, task aqdispatch.Task) (io.Reader, error) {
		if len(task.Payload) == 0 {
			t.Errorf("got empty Task=%#v", task)
		}
		type urlPayload struct {
			URL     string  `json:"url"`
			Payload payload `json:"payload"`
		}
		var p urlPayload
		if err := json.Unmarshal(task.Payload, &p); err != nil {
			return nil, fmt.Errorf("unmarshal %q: %w", string(task.Payload), err)
		}
		if p.Payload.ID == 0 {
			t.Errorf("zero ID in Task=%#v (%q)", task, string(task.Payload))
			return nil, fmt.Errorf("zero ID (%q)", string(task.Payload))
		}
		srvReceivedMu.Lock()
		ch := srvReceived[p.Payload.ID]
		delete(srvReceived, p.Payload.ID)
		srvReceivedMu.Unlock()
		answer := payload{ID: p.Payload.ID*2 + 1}
		select {
		case ch <- struct{}{}:
		default:
		}
		return nil, json.NewEncoder(w).Encode(answer)
	})
	defer closer()

	const parallel = 64

	call := func(ctx context.Context, i int) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ch := make(chan struct{}, 1)
		srvReceivedMu.Lock()
		srvReceived[i+1] = ch
		srvReceivedMu.Unlock()
		id, err := putMsg(ctx, tx, "_",
			fmt.Sprintf("test_%02d", i%parallel),
			payload{ID: i + 1},
			time.Second, false)
		if err != nil {
			return err
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		var p payload
		errMsg, err := getMsg(ctx, tx, id, 1*time.Second, &p)
		if err != nil {
			return err
		}
		if errMsg != "" {
			t.Log(errMsg)
		}
		return tx.Commit()
	}
	subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
	defer subCancel()
	qs := make([]string, 0, parallel)
	for i := range parallel {
		qs = append(qs, fmt.Sprintf("test_%02d", i))
	}
	go func() {
		for subCtx.Err() == nil {
			err := ex.Run(subCtx, qs)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return
				}
				t.Fatal(err)
			}
		}
	}()

	if resetTimer != nil {
		resetTimer()
	}
	var i int
	for loop() {
		for range 64 {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			err := call(ctx, i)
			cancel()
			if err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				t.Fatal(err)
			}
			i++
		}
	}
	subCancel()
	ex.Close()
}

const aqTableName, aqQueueName, aqQueueTypeName = "T_WSC_Q", "Q_WSC", "TY_WSC"

func setupAQ(b testing.TB, consume func(ctx context.Context, w io.Writer, task aqdispatch.Task) (io.Reader, error)) (context.Context, func() error, *sql.DB, *aqdispatch.Dispatcher) {
	{
		bb, err := json.Marshal(payload{ID: 42})
		if err != nil {
			b.Fatal(err)
		}
		var p payload
		if err = json.Unmarshal(bb, &p); err != nil {
			b.Fatal(err)
		}
		if p.ID != 42 {
			b.Fatalf("got %+v", p)
		}
	}
	logger := zlog.NewT(b).SLog()
	ctx := zlog.NewSContext(context.Background(), logger)
	tbc := make([]func() error, 0, 3)
	closer := func() error {
		var firstErr error
		for _, c := range tbc {
			if err := c(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
	tempDir, err := os.MkdirTemp("", "aqdispatch-*.test")
	if err != nil {
		closer()
		b.Fatal(err)
	}
	tbc = append(tbc, func() error { return os.RemoveAll(tempDir) })

	db, err := sqlOpen(ctx, *flagConnect)
	if err != nil {
		b.Fatal(err)
	}
	tbc = append(tbc, db.Close)

	if *flagSysConnect != "" {
		var usr string
		const qry = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL"
		if err := db.QueryRowContext(ctx, qry).Scan(&usr); err != nil {
			b.Fatalf("%s: %+v", qry, err)
		}
		db, err := sqlOpen(ctx, *flagSysConnect)
		if err != nil {
			b.Logf("WARN: connect to %q: %+v", *flagSysConnect, err)
		} else {
			func() {
				defer db.Close()
				for _, pkg := range []string{"DBMS_AQ", "DBMS_AQADM"} {
					qry := "GRANT EXECUTE ON " + pkg + " TO " + usr
					if _, err = db.ExecContext(ctx, qry); err != nil {
						b.Logf("WARN: %s: %+v", qry, err)
					}
				}
			}()
		}
	}

	{
		const qry = `DECLARE
PROCEDURE create_queue(p_drop_only IN BOOLEAN) IS
    c_tbl CONSTANT VARCHAR2(61) := :1;
    c_que CONSTANT VARCHAR2(61) := :2;
    c_typ CONSTANT VARCHAR2(61) := :3;
    c_owner CONSTANT VARCHAR2(128) := SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA');

    PROCEDURE del(tbl IN VARCHAR2, que IN VARCHAR2) IS
    BEGIN
      BEGIN SYS.DBMS_AQADM.stop_queue(que); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE( 'STOP('||que||'): '||SQLERRM ); END;
      BEGIN SYS.DBMS_AQADM.drop_queue(que); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE( 'DROP('||que||'): '||SQLERRM ); END;
      BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, force=>TRUE); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE( 'DROP('||tbl||'): '||SQLERRM ); END;
    END;
    PROCEDURE mk(typ IN VARCHAR2, tbl IN VARCHAR2, que IN VARCHAR2) IS
      v_priv_owner VARCHAR2(30) := 'ALL';
      v_priv_app VARCHAR2(30) := 'ALL';
    BEGIN
      DBMS_OUTPUT.PUT_LINE( 'CREATE_QUEUE_TABLE('''||tbl||''', '''||typ||''')');
      BEGIN SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, typ); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE( SQLERRM ); END;
      DBMS_OUTPUT.PUT_LINE( 'CREATE_QUEUE('''||que||''', '''||typ||''')');
      BEGIN SYS.DBMS_AQADM.CREATE_QUEUE(que, tbl); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE( SQLERRM ); END;
      IF que LIKE '%_REQ' THEN
        v_priv_owner := 'ENQUEUE'; v_priv_app := 'DEQUEUE';
      ELSIF que LIKE '%_RESP' THEN
        v_priv_owner := 'DEQUEUE'; v_priv_app := 'ENQUEUE';
      END IF;
      DBMS_OUTPUT.PUT_LINE( 'GRANT_QUEUE_PRIVILEGE('''||v_priv_owner||''', '''||que||''')');
      BEGIN SYS.DBMS_AQADM.GRANT_QUEUE_PRIVILEGE(v_priv_owner, que, c_owner); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE( SQLERRM ); END;
      DBMS_OUTPUT.PUT_LINE( 'GRANT_QUEUE_PRIVILEGE('''||v_priv_app||''', '''||que||''')');
      DBMS_OUTPUT.PUT_LINE( 'START_QUEUE('''||que||''')');
      BEGIN SYS.DBMS_AQADM.START_QUEUE(que); EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE( SQLERRM ); END;
    END;
  BEGIN
    del(c_tbl||'_REQ', c_que||'_REQ');
    del(c_tbl||'_RESP', c_que||'_RESP');
    IF p_drop_only THEN
      RETURN;
    END IF;
    BEGIN EXECUTE IMMEDIATE 'CREATE OR REPLACE TYPE ty_wsc_req AS OBJECT (NAME VARCHAR2(80), payload BLOB, aux BLOB)'; EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE( 'ty_wsc_req: '||SQLERRM); END;
    BEGIN EXECUTE IMMEDIATE 'CREATE OR REPLACE TYPE ty_wsc_resp AS OBJECT (errmsg VARCHAR2(4000), payload BLOB, aux BLOB)'; EXCEPTION WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE( 'ty_wsc_resp: '||SQLERRM); END;
    mk(c_typ||'_REQ', c_tbl||'_REQ', c_que||'_REQ');
    mk(c_typ||'_RESP', c_tbl||'_RESP', c_que||'_RESP');
  END create_queue;
BEGIN
  create_queue(FALSE);
END;`
		if _, err := db.ExecContext(ctx, qry, aqTableName, aqQueueName, aqQueueTypeName); err != nil {
			closer()
			b.Fatalf("%s: %+v", qry, err)
		}
	}
	aqLog := logger.WithGroup("aqdispatch")
	if !testing.Verbose() {
		aqLog = nil
	}
	ex, err := aqdispatch.New(db,
		aqdispatch.Config{
			Enc: encoding.Nop, QueueCount: 1, Concurrency: 3,
			DisQPath: tempDir, DisQPrefix: "aqdispatch-test-",
			DisQMaxMsgSize: 16 << 20, DisQMaxFileSize: 16 << 20,
			Timeout:     10 * time.Second,
			PipeTimeout: 10 * time.Second,
			Logger:      aqLog,
		},
		aqQueueName+"_REQ", aqQueueTypeName+"_REQ",
		func(ctx context.Context, w io.Writer, task aqdispatch.Task) (io.Reader, error) {
			return consume(ctx, w, task)
		},
		aqQueueName+"_RESP", aqQueueTypeName+"_RESP",
	)
	if err != nil {
		closer()
		b.Fatal(err)
	}
	tbc = append(tbc, ex.Close)

	return ctx, closer, db, ex

}

func putMsg(ctx context.Context, tx *sql.Tx, url, fun string, payload any, timeout time.Duration, waitCommit bool) (string, error) {
	const qry = `DECLARE
  v_url CONSTANT VARCHAR2(1000) := :url;
  v_func CONSTANT VARCHAR2(1000) := :func;
  v_payload JSON_OBJECT_T := JSON_OBJECT_T(:payload);
  v_timeout CONSTANT SIMPLE_INTEGER := :timeout;
  v_wait_commit CONSTANT BOOLEAN := (:waitCommit = 1);
  v_msg_id VARCHAR2(1000);

  FUNCTION aq_req_put(p_url IN VARCHAR2, p_func IN VARCHAR2,
                      p_payload IN OUT NOCOPY JSON_OBJECT_T, p_timeout IN PLS_INTEGER,
                      p_wait_commit IN BOOLEAN, p_delay IN PLS_INTEGER, p_priority IN PLS_INTEGER) RETURN VARCHAR2 IS
    enqueue_options    dbms_aq.enqueue_options_t;
    message_properties dbms_aq.message_properties_t;
    message_handle     RAW(16);
    v_json JSON_OBJECT_T := JSON_OBJECT_T();
    v_blob BLOB;
    c_timeout CONSTANT SIMPLE_INTEGER := 10;
  BEGIN
    enqueue_options.visibility := DBMS_AQ.immediate;
    IF p_wait_commit THEN
      enqueue_options.visibility := DBMS_AQ.on_commit;
    END IF;
    message_properties.expiration := NVL(p_timeout, c_timeout);
    message_properties.correlation := SYS_GUID;
    message_properties.priority := NVL(p_priority, 100);
    IF p_delay IS NOT NULL THEN
      message_properties.delay := p_delay;
    END IF;
    DBMS_LOB.createtemporary(v_blob, TRUE);
    v_json.put('url', p_url);
    v_json.put('payload', p_payload);
    v_json.to_blob(v_blob);
    DBMS_AQ.enqueue(
      queue_name         => '` + aqQueueName + `_REQ',
      enqueue_options    => enqueue_options,
      message_properties => message_properties,
      payload            => TY_wsc_req(NAME=>p_func, payload=>v_blob, aux=>v_blob),
      msgid              => message_handle);
    DBMS_LOB.freetemporary(v_blob);
    RETURN(message_properties.correlation);
  END;
BEGIN
  v_msg_id := aq_req_put(
    p_url=>v_url, p_func=>v_func, p_payload=>v_payload,
    p_timeout=>v_timeout, p_wait_commit=>v_wait_commit,
    p_delay=>0, p_priority=>1
  );
  :msgID := v_msg_id;
END;`
	var id string
	var msg []byte
	switch x := payload.(type) {
	case []byte:
		msg = x
	case string:
		msg = []byte(x)
	default:
		var err error
		if msg, err = json.Marshal(payload); err != nil {
			return "", err
		}
	}
	wc := 0
	if waitCommit {
		wc = 1
	}
	_, err := tx.ExecContext(
		ctx, qry, sql.Named("msgID", sql.Out{Dest: &id}),
		sql.Named("url", url), sql.Named("func", fun),
		sql.Named("payload", string(msg)),
		sql.Named("timeout", int32(timeout/time.Second)),
		sql.Named("waitCommit", wc),
	)
	if err != nil {
		err = fmt.Errorf("%s: %w", qry, err)
	}
	return id, err
}

func getMsg(ctx context.Context, tx *sql.Tx, refID string, timeout time.Duration, dest any) (string, error) {
	const qry = `DECLARE
  v_ref_id CONSTANT VARCHAR2(32) := :refID;
  v_timeout CONSTANT PLS_INTEGER := :timeout;
  v_payload JSON_OBJECT_T;
  
  PROCEDURE aq_resp_get(p_ref_id IN VARCHAR2,
                        p_error_message OUT NOCOPY VARCHAR2, p_payload OUT NOCOPY JSON_OBJECT_T,
                        p_timeout IN PLS_INTEGER) IS
    dequeue_options    dbms_aq.dequeue_options_t;
    message_properties dbms_aq.message_properties_t;
    msg TY_wsc_resp;
    v_msg_id RAW(16);
    c_timeout CONSTANT SIMPLE_INTEGER := 10;

    json_decode_error EXCEPTION;
    PRAGMA EXCEPTION_INIT(json_decode_error, -40587 );
    no_messages EXCEPTION;
    PRAGMA EXCEPTION_INIT(no_messages, -25228 );
  BEGIN
    p_payload := NULL;
    dequeue_options.wait := NVL(p_timeout, c_timeout);
    dequeue_options.correlation := p_ref_id;
    dequeue_options.navigation := DBMS_AQ.FIRST_MESSAGE;
    dequeue_options.dequeue_mode := DBMS_AQ.REMOVE;
    dbms_aq.dequeue(
      queue_name         => '` + aqQueueName + `_RESP',
      dequeue_options    => dequeue_options,
      message_properties => message_properties,
      payload            => msg,
      msgid              => v_msg_id);
    p_error_message := msg.errmsg;
    IF msg.payload IS NOT NULL AND DBMS_LOB.getlength(msg.payload) > 0 THEN
      BEGIN
        p_payload := JSON_OBJECT_T.parse(msg.payload);
      EXCEPTION WHEN json_decode_error THEN
        RAISE;
      END;
    END IF;
  --  RETURN(0);
  EXCEPTION WHEN no_messages THEN NULL;
  --  RETURN(-1);
  END;
BEGIN
  aq_resp_get(v_ref_id, :errMsg, v_payload, v_timeout);
  IF v_payload IS NULL THEN
    :payload := NULL;
  ELSE
    :payload := v_payload.to_string;
  END IF;
END;`
	var errMsg string
	var payload string
	if _, err := tx.ExecContext(
		ctx, qry,
		sql.Named("refID", refID),
		sql.Named("errMsg", sql.Out{Dest: &errMsg}),
		sql.Named("payload", sql.Out{Dest: &payload}),
		sql.Named("timeout", int32(timeout/time.Second)),
	); err != nil {
		return "", fmt.Errorf("%s: %w", qry, err)
	}
	if payload == "" {
		return errMsg, errNotFound
	}
	var err error
	if err = json.Unmarshal([]byte(payload), dest); err != nil {
		err = fmt.Errorf("unmarshal %q: %w", payload, err)
	}
	return errMsg, err
}

var errNotFound = errors.New("not found")

func nvl[T comparable](a T, b ...T) T {
	var z T
	if a != z {
		return a
	}
	for _, a := range b {
		if a != z {
			return a
		}
	}
	return a
}

func sqlOpen(ctx context.Context, dsn string) (*sql.DB, error) {
	P, err := godror.ParseConnString(dsn)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(godror.NewConnector(P))
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	err = db.PingContext(ctx)
	cancel()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("ping %+v: %w", P, err)
	}
	db.SetMaxOpenConns(4)
	return db, nil
}
