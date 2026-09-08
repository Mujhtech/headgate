module github.com/mujhtech/headgate/go/headgateapi

go 1.27.0

replace github.com/mujhtech/headgate/go => ../

replace github.com/mujhtech/headgate/go/headgateworkflow => ../headgateworkflow

require (
	github.com/mujhtech/headgate/go v0.1.8
	github.com/mujhtech/headgate/go/driver/headgatemysql v0.1.8
	github.com/mujhtech/headgate/go/driver/headgatepgx v0.1.8
	github.com/mujhtech/headgate/go/driver/headgateredis v0.1.8
	github.com/mujhtech/headgate/go/headgateui v0.1.8
	github.com/mujhtech/headgate/go/headgateworkflow v0.1.8
)

require (
	cel.dev/cel-go v0.32.0 // indirect
	cel.dev/expr v0.25.1 // indirect
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/redis/go-redis/v9 v9.22.0 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/exp v0.0.0-20240823005443-9b4947da3948 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240826202546-f6391c0de4c7 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240826202546-f6391c0de4c7 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/mujhtech/headgate/go/driver/headgatepgx => ../driver/headgatepgx

replace github.com/mujhtech/headgate/go/driver/headgateredis => ../driver/headgateredis

replace github.com/mujhtech/headgate/go/driver/headgatemysql => ../driver/headgatemysql

replace github.com/mujhtech/headgate/go/headgateui => ../headgateui

replace github.com/mujhtech/headgate/go/headgatetest => ../headgatetest

replace github.com/mujhtech/headgate/go/headgatemigrate => ../headgatemigrate
