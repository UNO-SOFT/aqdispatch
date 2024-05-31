// Copyright 2021, 2024 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package aqdispatch

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"google.golang.org/protobuf/proto"
	timestamp "google.golang.org/protobuf/types/known/timestamppb"
)

func TestPBMarshal(t *testing.T) {
	task := getTask()
	B := MarshalProtobuf(nil, task)

	pt := toPBTask(task)
	pbB, err := proto.Marshal(&pt)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(B, pbB) {
		t.Log("bytes not equal")
	}

	var got Task
	if err = got.UnmarshalProtobuf(pbB); err != nil {
		t.Fatal(err)
	}
	want := task
	if d := cmp.Diff(got, want); d != "" {
		t.Error(d)
		t.Fatalf("got %#v, wanted %#v", got, want)
	}
	gotB, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	wantB, err := json.MarshalIndent(want, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(gotB))
	if !bytes.Equal(gotB, wantB) {
		t.Fatalf("got %s, wanted %s", gotB, wantB)
	}
}

func BenchmarkPBMarshal(t *testing.B) {
	t.Run("marshal", func(t *testing.B) {
		task := toPBTask(getTask())
		t.ReportAllocs()
		t.ResetTimer()
		for i := 0; i < t.N; i++ {
			b, err := proto.Marshal(&task)
			if err != nil {
				t.Fatal(err)
			}
			t.SetBytes(int64(len(b)))
		}
	})

	t.Run("unmarshal", func(t *testing.B) {
		task := toPBTask(getTask())
		b, err := proto.Marshal(&task)
		if err != nil {
			t.Fatal(err)
		}
		t.ReportAllocs()
		t.ResetTimer()
		for i := 0; i < t.N; i++ {
			var task pbTask
			if err := proto.Unmarshal(b, &task); err != nil {
				t.Fatal(err)
			}
			t.SetBytes(int64(len(task.Payload)))
		}
	})
}

func BenchmarkEasyMarshal(t *testing.B) {
	t.Run("marshal", func(t *testing.B) {
		task := getTask()
		t.ReportAllocs()
		t.ResetTimer()
		var b []byte
		for i := 0; i < t.N; i++ {
			b = MarshalProtobuf(b[:], task)
			t.SetBytes(int64(len(b)))
		}
	})

	t.Run("unmarshal", func(t *testing.B) {
		b := MarshalProtobuf(nil, getTask())
		t.ReportAllocs()
		t.ResetTimer()
		for i := 0; i < t.N; i++ {
			var task Task
			if err := task.UnmarshalProtobuf(b); err != nil {
				t.Fatal(err)
			}
			t.SetBytes(int64(len(task.Payload)))
		}
	})
}

func getTask() Task {
	var payload, _ = io.ReadAll(base64.NewDecoder(base64.StdEncoding, strings.NewReader(payloadB64)))
	payload2 := payload[128:512]
	payload3 := payload[512:]
	payload = payload[:128]

	return Task{
		Name:     "árvíztűrő tükörfúrógép",
		RefID:    "01HZ6FMWHDBFS463VVV2WNHK9Y",
		Deadline: time.Now().Add(time.Minute),
		Payload:  payload,
		Blobs: []*Blob{
			&Blob{Bytes: payload2},
			&Blob{Bytes: payload3},
		},
	}
}
func toPBTask(task Task) pbTask {
	return pbTask{
		Name:     task.Name,
		RefID:    task.RefID,
		Deadline: timestamp.New(task.Deadline),
		Payload:  task.Payload,
		Blobs: []*pbBlob{
			&pbBlob{Bytes: task.Blobs[0].Bytes},
			&pbBlob{Bytes: task.Blobs[1].Bytes},
		},
	}
}

const payloadB64 = `krmMEyQyg2awN2ekw//1aw/w6f6Fpob7k4shyEeU8YmdchqNd20NSECOIperY1mT14wQ+N6R+b2u
SfV1bajer5v7EsoHREQ987Yol5f+M4B3xESq25/b9luXpFKRlB+jxoXfRwbNK79c+nRFua0I1jlD
8ViMq0Rm3JDvBYA7nhrX+dN3LhWtwua5qxujiFYajLsNbUcIkJiZY5eUSYGgrC92wtVgbyrrJTSv
ThzGZjXVFc90YYjKSp2DFdwFEO1pvhAeSJXFEOctc/80TJlc2REkIOn2xQoSF3TyDKMhRzlaV+0N
r+UN9ICL/aXP2QiL1zBq/K04eGlIQZX00jnnsSeZJkaFAUsP91MHLKJRqvJjES8XAYADN0WNOmWp
LV+3PAAzxMvz6IFna8vSWsGseX5KPH/IVNSUMEX098Jf9g8yd0OMrEPoZ9mve+TaT8/W18gPg0Ch
1J73xAR4SC33kQB43c6SDpFKFlCwIaeugMfFUp6gyIpZZpgWYCl4ZS4/URNfKkd5Zxzni6L/43kv
Vd5L5L8oO3/C8ejaBSF530k1ETPnL2w0cH/e/jMyjr0XzYCA5BVkc2LOeRuvBaqYvUiBpZhyb4H0
jVshFbq4qN8lnqhbYuzLUAxSGqpg1BOx3O5ezgfJkpbpx1NZh0Dj4wm9ovcgC0cVmcVPLfAO64L3
xEDujDjHjTnSkBeqytXynVvEpyFGiUtByDBU7h6cGqtFwCmdQeVGFSNeNzn+nJFu2v9Y4ukPp4Tf
adpBt34AcQonN642jdGJkNuvmpifTSt16F6sS9CPWX5QWtzIQk/K2aGqj46YkZEJ9uQ1N40OCK8R
9zCMx4K5NtI6B9ZMwzwvLyKK/O0ywaETFbVlEm1kgSMbvsqGWDG5FMA2WBCsrpjC8jDoGqh1H51J
o6aIUlCSykjCRBsp/k23s28P5uem7ccppaAvwLd+kv/JxKVIEID9fLuqhqZIyIH6gC9c4EeimAR+
J9yOvUOrn+jlIH1W+UTq8YSwXfL5on8gcw281Uv0/6LvuoRZVpxL8oJhA+83kw0Zq5kvIYuC4MuO
lxzray2/jIHM0bMM8Wtcd81/vQbvXIlSKVSmsRsOQ8AT6HHMry5kDgQXq22YxCgDMtUStbZkCK2F
QbI/FSCkj6VnhHEjaGMw9rBzahrX37v8BxsJ2yr5nGzVDFlt+NRNLGig+7iDrzsM9K7DcAm3D7Ls
iw7jqOzx6ULzqGsGy3itJiuA4EiRoSHF/ij9RSQGCuJFLxmyjF41QS1HfY/uXqwslG6i97juoWII
s4f6sz4iAWjhigJL3X9ov0M/nzdlU7fPi1wSlPtwZCiIZZUrDYvBwP18JNfRuUl2la103FPFdQ==`
