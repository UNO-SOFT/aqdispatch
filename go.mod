module github.com/UNO-SOFT/aqdispatch

go 1.21

toolchain go1.22.3

require (
	github.com/VictoriaMetrics/easyproto v0.1.4
	github.com/godror/godror v0.40.2
	github.com/google/go-cmp v0.6.0
	github.com/nsqio/go-diskqueue v1.1.0
	golang.org/x/sync v0.7.0
	golang.org/x/text v0.15.0
	google.golang.org/protobuf v1.34.1
)

require (
	github.com/UNO-SOFT/otel v0.8.4 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/godror/knownpb v0.1.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.52.0 // indirect
	go.opentelemetry.io/otel v1.27.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.27.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.27.0 // indirect
	go.opentelemetry.io/otel/metric v1.27.0 // indirect
	go.opentelemetry.io/otel/sdk v1.27.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.27.0 // indirect
	go.opentelemetry.io/otel/trace v1.27.0 // indirect
	golang.org/x/exp v0.0.0-20230817173708-d852ddb80c63 // indirect
	golang.org/x/sys v0.20.0 // indirect
)

retract v0.3.5

//replace github.com/godror/godror => ../../godror/godror
