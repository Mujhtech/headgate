module github.com/mujhtech/headgate/go/headgateapi

go 1.27.0

replace github.com/mujhtech/headgate/go => ../

require (
	github.com/mujhtech/headgate/go v0.1.6
	github.com/mujhtech/headgate/go/driver/headgatemysql v0.1.6
	github.com/mujhtech/headgate/go/driver/headgatepgx v0.1.6
	github.com/mujhtech/headgate/go/driver/headgateredis v0.1.6
	github.com/mujhtech/headgate/go/headgateui v0.1.6
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/redis/go-redis/v9 v9.22.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/mujhtech/headgate/go/driver/headgatepgx => ../driver/headgatepgx

replace github.com/mujhtech/headgate/go/driver/headgateredis => ../driver/headgateredis

replace github.com/mujhtech/headgate/go/driver/headgatemysql => ../driver/headgatemysql

replace github.com/mujhtech/headgate/go/headgateui => ../headgateui
