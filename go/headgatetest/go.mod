module github.com/mujhtech/headgate/go/headgatetest

go 1.25.0

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/jackc/pgx/v5 v5.9.2
	github.com/mujhtech/headgate/go v0.1.7
	github.com/mujhtech/headgate/go/headgatemigrate v0.1.7
	github.com/redis/go-redis/v9 v9.7.3
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	golang.org/x/text v0.40.0 // indirect
)

replace github.com/mujhtech/headgate/go => ..

replace github.com/mujhtech/headgate/go/headgatemigrate => ../headgatemigrate
