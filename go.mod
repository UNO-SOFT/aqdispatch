module github.com/UNO-SOFT/aqdispatch

go 1.23.0

toolchain go1.24.1

require (
	github.com/VictoriaMetrics/easyproto v0.1.4
	github.com/godror/godror v0.48.1
	github.com/google/go-cmp v0.6.0
	github.com/nsqio/go-diskqueue v1.1.0
	golang.org/x/sync v0.13.0
	golang.org/x/text v0.24.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/godror/knownpb v0.2.0 // indirect
	github.com/planetscale/vtprotobuf v0.6.0 // indirect
	golang.org/x/exp v0.0.0-20250305212735-054e65f0b394 // indirect
)

retract v0.3.5

//replace github.com/godror/godror => ../../godror/godror
