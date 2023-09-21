module github.com/UNO-SOFT/aqdispatch

go 1.17

require (
	github.com/godror/godror v0.40.2
	github.com/nsqio/go-diskqueue v1.1.0
	golang.org/x/sync v0.3.0
	golang.org/x/text v0.12.0
	google.golang.org/protobuf v1.31.0
)

require (
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/godror/knownpb v0.1.1 // indirect
	golang.org/x/exp v0.0.0-20230817173708-d852ddb80c63 // indirect
)

retract v0.3.5

//replace github.com/godror/godror => ../../godror/godror
