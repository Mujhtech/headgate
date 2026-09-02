module github.com/mujhtech/headgate/go/headgatemigrate

go 1.27.0

require (
	github.com/go-sql-driver/mysql v1.10.1
	github.com/jackc/pgx/v5 v5.10.0
	github.com/mujhtech/headgate/go v0.1.6
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/mujhtech/headgate/go => ..
