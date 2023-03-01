module github.com/UNO-SOFT/aqdispatch

go 1.17

require (
	github.com/go-logr/logr v1.2.3
	github.com/godror/godror v0.36.0
	github.com/nsqio/go-diskqueue v1.1.0
	golang.org/x/sync v0.1.0
	golang.org/x/text v0.7.0
	google.golang.org/protobuf v1.28.1
)

require (
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/godror/knownpb v0.1.1 // indirect
)

retract v0.3.5

//replace github.com/godror/godror => ../../godror/godror
