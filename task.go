package aqdispatch

import (
	"time"
)

type Task struct {
	Name     string
	RefID    string
	Deadline time.Time
	Payload  []byte
	Blobs    []*Blob
}
type Timestamp struct {
	// seconds	int64	Represents seconds of UTC time since Unix epoch 1970-01-01T00:00:00Z. Must be from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59Z inclusive.
	Seconds int64 `protobuf:"int64,1,opt,name=Seconds,proto3"`
	// nanos	int32	Non-negative fractions of a second at nanosecond resolution. Negative second values with fractions must still have non-negative nanos values that count forward in time. Must be from 0 to 999,999,999 inclusive.
	Nanos int32 `protobuf:"int32,2,opt,name=Nanos,proto3"`
}

func (ts Timestamp) Time() time.Time { return time.Unix(ts.Seconds, int64(ts.Nanos)) }

type Blob struct {
	Bytes []byte
}
