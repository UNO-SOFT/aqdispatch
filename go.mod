module github.com/UNO-SOFT/aqdispatch

go 1.23.0

toolchain go1.24.1

require (
	github.com/UNO-SOFT/zlog v0.8.1
	github.com/VictoriaMetrics/easyproto v0.1.4
	github.com/godror/godror v0.49.0
	github.com/google/go-cmp v0.6.0
	github.com/nsqio/go-diskqueue v1.1.0
	golang.org/x/sync v0.14.0
	golang.org/x/text v0.25.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/godror/knownpb v0.3.0 // indirect
	golang.org/x/exp v0.0.0-20250506013437-ce4c2cf36ca6 // indirect
	golang.org/x/sys v0.22.0 // indirect
	golang.org/x/term v0.10.0 // indirect
)

retract v0.3.5

//replace github.com/godror/godror => ../../godror/godror
