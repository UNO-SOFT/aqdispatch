module github.com/UNO-SOFT/aqdispatch

go 1.22.4

toolchain go1.23.4

require (
	github.com/VictoriaMetrics/easyproto v0.1.4
	github.com/godror/godror v0.46.0
	github.com/google/go-cmp v0.6.0
	github.com/nsqio/go-diskqueue v1.1.0
	golang.org/x/sync v0.11.0
	golang.org/x/text v0.22.0
	google.golang.org/protobuf v1.36.4
)

require (
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/godror/knownpb v0.2.0 // indirect
	github.com/planetscale/vtprotobuf v0.6.0 // indirect
	golang.org/x/exp v0.0.0-20250128182459-e0ece0dbea4c // indirect
)

retract v0.3.5

//replace github.com/godror/godror => ../../godror/godror
