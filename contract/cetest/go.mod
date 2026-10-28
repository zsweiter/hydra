// Module cetest checks that hydra interoperates with the
// official CloudEvents SDK. It is a module of its own so the SDK
// never becomes a dependency of hydra or hydra-scheduler.
module github.com/zsweiter/hydra/contract/cetest

go 1.27.3

require (
	github.com/cloudevents/sdk-go/protocol/nats_jetstream/v2 v2.16.2
	github.com/cloudevents/sdk-go/v2 v2.16.2
	github.com/nats-io/nats.go v1.45.0
	github.com/zsweiter/hydra v0.2.0
)

require (
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
	github.com/teambition/rrule-go v1.8.2 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
)

replace github.com/zsweiter/hydra v0.2.0 => ../..
