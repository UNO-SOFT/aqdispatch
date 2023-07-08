module github.com/UNO-SOFT/aqdispatch

go 1.17

require (
	github.com/godror/godror v0.37.1-0.20230708063457-1527aad07106
	github.com/nsqio/go-diskqueue v1.1.0
	golang.org/x/exp v0.0.0-20230515195305-f3d0a9c9a5cc
	golang.org/x/sync v0.2.0
	golang.org/x/text v0.9.0
	google.golang.org/protobuf v1.30.0
)

require (
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/godror/knownpb v0.1.1 // indirect
)

retract v0.3.5

//replace github.com/godror/godror => ../../godror/godror
