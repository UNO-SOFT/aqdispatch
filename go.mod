module github.com/UNO-SOFT/aqdispatch

go 1.17

require (
	github.com/go-logr/logr v1.2.3
	github.com/godror/godror v0.32.1
	github.com/nsqio/go-diskqueue v1.1.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/text v0.3.7
	google.golang.org/protobuf v1.28.0
)

require (
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/godror/knownpb v0.1.0 // indirect
)

retract v0.3.5

//replace github.com/godror/godror => ../../godror/godror
