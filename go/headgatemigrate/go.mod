module github.com/mujhtech/headgate/headgatemigrate

go 1.24

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/jackc/pgx/v5 v5.7.2
	github.com/mujhtech/headgate v0.0.0
)

replace github.com/mujhtech/headgate => ..
