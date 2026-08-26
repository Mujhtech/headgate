module github.com/mujhtech/headgate/headgatetest

go 1.24

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/jackc/pgx/v5 v5.7.2
	github.com/mujhtech/headgate v0.0.0
	github.com/mujhtech/headgate/headgatemigrate v0.0.0
	github.com/redis/go-redis/v9 v9.7.0
)

replace github.com/mujhtech/headgate => ..

replace github.com/mujhtech/headgate/headgatemigrate => ../headgatemigrate
