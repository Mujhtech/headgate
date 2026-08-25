module github.com/mujhtech/headgate/driver/headgatemysql

go 1.24

require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/mujhtech/headgate v0.0.0
	github.com/mujhtech/headgate/headgatemigrate v0.0.0
	github.com/mujhtech/headgate/headgatetest v0.0.0
)

require filippo.io/edwards25519 v1.1.0 // indirect

replace github.com/mujhtech/headgate => ../..

replace github.com/mujhtech/headgate/headgatemigrate => ../../headgatemigrate

replace github.com/mujhtech/headgate/headgatetest => ../../headgatetest
