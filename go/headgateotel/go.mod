module github.com/mujhtech/headgate/headgateotel

go 1.24.0

require (
	github.com/mujhtech/headgate v0.0.0
	go.opentelemetry.io/otel v1.41.0
	go.opentelemetry.io/otel/metric v1.41.0
	go.opentelemetry.io/otel/sdk v1.41.0
	go.opentelemetry.io/otel/sdk/metric v1.41.0
	go.opentelemetry.io/otel/trace v1.41.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/mujhtech/headgate/headgatetest v0.0.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
)

require (
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.21.0 // indirect
)

replace github.com/mujhtech/headgate => ..

replace github.com/mujhtech/headgate/headgatemigrate => ../headgatemigrate

replace github.com/mujhtech/headgate/headgatetest => ../headgatetest
