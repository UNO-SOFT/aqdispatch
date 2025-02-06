package aqdispatch

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/VictoriaMetrics/easyproto"
)

type Task struct {
	Name     string    `protobuf:"bytes,1,opt,name=Name,proto3" json:"Name,omitempty"`
	RefID    string    `protobuf:"bytes,2,opt,name=RefID,proto3" json:"RefID,omitempty"`
	Deadline time.Time `protobuf:"bytes,4,opt,name=Deadline,proto3" json:"Deadline,omitempty"`
	Payload  []byte    `protobuf:"bytes,5,opt,name=Payload,proto3" json:"Payload,omitempty"`
	Blobs    []*Blob   `protobuf:"bytes,6,rep,name=Blobs,proto3" json:"Blobs,omitempty"`
}

func (t Task) IsZero() bool { return t.Name == "" }

type Timestamp struct {
	// seconds	int64	Represents seconds of UTC time since Unix epoch 1970-01-01T00:00:00Z. Must be from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59Z inclusive.
	Seconds int64 `protobuf:"int64,1,opt,name=Seconds,proto3"`
	// nanos	int32	Non-negative fractions of a second at nanosecond resolution. Negative second values with fractions must still have non-negative nanos values that count forward in time. Must be from 0 to 999,999,999 inclusive.
	Nanos int32 `protobuf:"int32,2,opt,name=Nanos,proto3"`
}

func (ts Timestamp) Time() time.Time { return time.Unix(ts.Seconds, int64(ts.Nanos)) }

type Blob struct {
	Bytes []byte `protobuf:"bytes,1,opt,name=Bytes,proto3" json:"Bytes,omitempty"`
}

var mp easyproto.MarshalerPool

func MarshalProtobuf(dst []byte, ms protobufMarshaler) []byte {
	m := mp.Get()
	ms.marshalProtobuf(m.MessageMarshaler())
	dst = m.Marshal(dst)
	mp.Put(m)
	return dst
}

type protobufMarshaler interface {
	marshalProtobuf(*easyproto.MessageMarshaler)
}

func (t Task) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendString(1, t.Name)
	mm.AppendString(2, t.RefID)
	// newTimestamp(t.Deadline).marshalProtobuf(mm.AppendMessage(4))
	timeMarshalProtobuf(mm.AppendMessage(4), t.Deadline)
	mm.AppendBytes(5, t.Payload)
	for _, b := range t.Blobs {
		b.marshalProtobuf(mm.AppendMessage(6))
	}
}

func timeMarshalProtobuf(mm *easyproto.MessageMarshaler, t time.Time) {
	mm.AppendInt64(1, t.Unix())
	mm.AppendInt32(2, int32(t.Nanosecond()))
}

// func newTimestamp(t time.Time) Timestamp { return Timestamp{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())} }

// func (ts Timestamp) marshalProtobuf(mm *easyproto.MessageMarshaler) { mm.AppendInt64(1, ts.Seconds); mm.AppendInt32(2, ts.Nanos) }

func (b Blob) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendBytes(1, b.Bytes)
}

// UnmarshalProtobuf unmarshals ts from protobuf message at src.
func (ts *Task) UnmarshalProtobuf(src []byte) (err error) {
	// Set default Timeseries values
	*ts = Task{}

	// Parse Timeseries message at src
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Timeseries message")
		}
		switch fc.FieldNum {
		case 1:
			if name, ok := fc.String(); !ok {
				return fmt.Errorf("cannot read Task name")
			} else {
				ts.Name = strings.Clone(name)
			}
		case 2:
			if refID, ok := fc.String(); !ok {
				return fmt.Errorf("cannot read Task refID")
			} else {
				ts.RefID = strings.Clone(refID)
			}
		case 4:
			if data, ok := fc.MessageData(); !ok {
				return fmt.Errorf("cannot read Task deadline")
			} else {
				var t Timestamp
				if err := t.UnmarshalProtobuf(data); err != nil {
					return fmt.Errorf("cannot unmarshal deadline: %w", err)
				}
				ts.Deadline = t.Time()
			}
		case 5:
			if b, ok := fc.Bytes(); !ok {
				return fmt.Errorf("cannot read Task payload")
			} else {
				ts.Payload = bytes.Clone(b)
			}
		case 6:
			if data, ok := fc.MessageData(); !ok {
				return fmt.Errorf("cannot read Task deadline")
			} else {
				var b Blob
				if err := b.UnmarshalProtobuf(data); err != nil {
					return fmt.Errorf("cannot unmarshal blob: %w", err)
				} else {
					ts.Blobs = append(ts.Blobs, &b)
				}
			}
		}
	}
	return nil
}

// UnmarshalProtobuf unmarshals ts from protobuf message at src.
func (ts *Timestamp) UnmarshalProtobuf(src []byte) (err error) {
	// Set default Timeseries values
	*ts = Timestamp{}

	// Parse Timeseries message at src
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Timeseries message")
		}
		switch fc.FieldNum {
		case 1:
			if i, ok := fc.Int64(); !ok {
				return fmt.Errorf("cannot unmarshal timestamp seconds")
			} else {
				ts.Seconds = i
			}
		case 2:
			if i, ok := fc.Int32(); !ok {
				return fmt.Errorf("cannot unmarshal timestamp seconds")
			} else {
				ts.Nanos = i
			}
		}
	}
	return nil
}

// UnmarshalProtobuf unmarshals ts from protobuf message at src.
func (B *Blob) UnmarshalProtobuf(src []byte) (err error) {
	// Set default Timeseries values
	B.Bytes = nil

	// Parse Timeseries message at src
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Timeseries message")
		}
		switch fc.FieldNum {
		case 1:
			b, ok := fc.Bytes()
			if !ok {
				return fmt.Errorf("cannot unmarshal blob bytes: %w", err)
			}
			B.Bytes = bytes.Clone(b)
		}
	}
	return nil
}
