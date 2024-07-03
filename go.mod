module github.com/UNO-SOFT/aqdispatch

go 1.21

toolchain go1.22.3

require (
	github.com/godror/godror v0.44.1
	golang.org/x/sync v0.7.0
	golang.org/x/text v0.16.0
)

require (
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/godror/knownpb v0.1.1 // indirect
	golang.org/x/exp v0.0.0-20240613232115-7f521ea00fb8 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

retract v0.3.5

//replace github.com/godror/godror => ../../godror/godror
