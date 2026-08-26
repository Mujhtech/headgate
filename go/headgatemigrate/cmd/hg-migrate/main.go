package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/mujhtech/headgate/headgatemigrate"
	"github.com/mujhtech/headgate/postgressql"
)

type command string

const (
	up       command = "up"
	down     command = "down"
	validate command = "validate"
	list     command = "list"
	get      command = "get"
	version  command = "version"
	adopt    command = "adopt"
)

type cli struct {
	backend     headgatemigrate.Backend
	databaseURL string
	schema      string
	lockNS      string
	command     command
	options     headgatemigrate.Options
	getVersion  int
	getDir      headgatemigrate.Direction
}

const usage = `hg-migrate [--database-url URL] [--backend postgres|mysql] [--schema NAME] [--lock-namespace NAME] COMMAND [OPTIONS]

Commands:
  up          apply versions (default target: latest)
  down        roll versions back; requires --confirm unless --dry-run
  validate    verify history checksums and the current schema manifest
  list        list embedded and applied versions
  get         print one embedded migration's SQL
  version     print current and latest versions
  adopt       record a validated unversioned current schema; requires --confirm

Options:
  --schema NAME        explicit Postgres schema (qualified; never search_path)
  --lock-namespace N   MySQL migration lock namespace (up/down/adopt only)
  --target-version N   desired version after up/down
  --max-steps N        bound versions applied by this invocation
  --dry-run            plan without creating history or changing schema
  --version N          migration version for get
  --up | --down        SQL direction for get (default: up)
  --confirm            acknowledge a destructive down or schema adoption

Postgres --schema explicitly qualifies every object and requires a pre-created schema.
MySQL instances are selected by the database in their URL.

HG_DATABASE_URL and DATABASE_URL are used when --database-url is omitted.`

func popValue(args *[]string, index int, flag string) (string, error) {
	if index+1 >= len(*args) {
		return "", fmt.Errorf("%s requires a value", flag)
	}
	value := (*args)[index+1]
	*args = append((*args)[:index], (*args)[index+2:]...)
	return value, nil
}

func parseBackend(value string) (headgatemigrate.Backend, error) {
	switch value {
	case "postgres", "postgresql", "pg":
		return headgatemigrate.Postgres, nil
	case "mysql":
		return headgatemigrate.MySQL, nil
	default:
		return "", fmt.Errorf("unknown backend %q; want postgres or mysql", value)
	}
}

func inferBackend(databaseURL string) headgatemigrate.Backend {
	switch {
	case strings.HasPrefix(databaseURL, "postgres://"), strings.HasPrefix(databaseURL, "postgresql://"):
		return headgatemigrate.Postgres
	case strings.HasPrefix(databaseURL, "mysql://"):
		return headgatemigrate.MySQL
	default:
		return ""
	}
}

func parseCLI(args []string, getenv func(string) string) (cli, error) {
	if len(args) == 0 {
		return cli{}, errors.New("missing command")
	}
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			return cli{}, errors.New(usage)
		}
	}
	var databaseURL string
	var backend headgatemigrate.Backend
	var schema string
	var lockNS string
	for index := 0; index < len(args); {
		switch args[index] {
		case "--database-url":
			value, err := popValue(&args, index, "--database-url")
			if err != nil {
				return cli{}, err
			}
			databaseURL = value
		case "--backend":
			value, err := popValue(&args, index, "--backend")
			if err != nil {
				return cli{}, err
			}
			backend, err = parseBackend(value)
			if err != nil {
				return cli{}, err
			}
		case "--schema":
			value, err := popValue(&args, index, "--schema")
			if err != nil {
				return cli{}, err
			}
			schema = value
		case "--lock-namespace":
			value, err := popValue(&args, index, "--lock-namespace")
			if err != nil {
				return cli{}, err
			}
			lockNS = value
		default:
			index++
		}
	}
	if databaseURL == "" {
		databaseURL = getenv("HG_DATABASE_URL")
		if databaseURL == "" {
			databaseURL = getenv("DATABASE_URL")
		}
	}
	if len(args) == 0 {
		return cli{}, errors.New("missing command")
	}
	cmd := command(args[0])
	args = args[1:]
	switch cmd {
	case up, down, validate, list, get, version, adopt:
	default:
		return cli{}, fmt.Errorf("unknown command %q", cmd)
	}

	var options headgatemigrate.Options
	getVersion := 0
	getDir := headgatemigrate.Up
	confirm := false
	for len(args) != 0 {
		switch args[0] {
		case "--target-version":
			value, err := popValue(&args, 0, "--target-version")
			if err != nil {
				return cli{}, err
			}
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 0 {
				return cli{}, fmt.Errorf("invalid --target-version %q; want a non-negative integer", value)
			}
			options.TargetVersion = &parsed
		case "--max-steps":
			value, err := popValue(&args, 0, "--max-steps")
			if err != nil {
				return cli{}, err
			}
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 0 {
				return cli{}, fmt.Errorf("invalid --max-steps %q; want a non-negative integer", value)
			}
			options.MaxSteps = &parsed
		case "--version":
			value, err := popValue(&args, 0, "--version")
			if err != nil {
				return cli{}, err
			}
			getVersion, err = strconv.Atoi(value)
			if err != nil || getVersion <= 0 {
				return cli{}, fmt.Errorf("invalid --version %q; want a positive integer", value)
			}
		case "--dry-run":
			options.DryRun = true
			args = args[1:]
		case "--confirm":
			confirm = true
			args = args[1:]
		case "--up":
			getDir = headgatemigrate.Up
			args = args[1:]
		case "--down":
			getDir = headgatemigrate.Down
			args = args[1:]
		default:
			return cli{}, fmt.Errorf("unknown option %q", args[0])
		}
	}
	if cmd == get && getVersion == 0 {
		return cli{}, errors.New("get requires --version N")
	}
	if cmd == down && !options.DryRun && !confirm {
		return cli{}, errors.New("down is destructive; pass --confirm (or inspect it with --dry-run)")
	}
	if cmd == adopt && !confirm {
		return cli{}, errors.New("adopt writes migration history; pass --confirm after reviewing validate")
	}
	if backend == "" {
		backend = inferBackend(databaseURL)
	}
	if backend == "" {
		return cli{}, errors.New("cannot infer backend; pass --backend postgres|mysql")
	}
	if cmd != get && databaseURL == "" {
		return cli{}, errors.New("missing --database-url (or HG_DATABASE_URL / DATABASE_URL)")
	}
	if schema != "" && backend != headgatemigrate.Postgres {
		return cli{}, errors.New("--schema is Postgres-only; select a MySQL database in its URL")
	}
	if schema != "" {
		if _, err := postgressql.NewNamespace(schema); err != nil {
			return cli{}, err
		}
	}
	if lockNS != "" && backend != headgatemigrate.MySQL {
		return cli{}, errors.New("--lock-namespace is MySQL-only; Postgres uses schema-local table locks")
	}
	if lockNS != "" && cmd != up && cmd != down && cmd != adopt {
		return cli{}, errors.New("--lock-namespace applies only to MySQL up, down, and adopt")
	}
	if lockNS != "" {
		if _, err := headgatemigrate.MySQLMigrationLockName(lockNS, "validation"); err != nil {
			return cli{}, err
		}
	}
	return cli{
		backend: backend, databaseURL: databaseURL, schema: schema, lockNS: lockNS, command: cmd,
		options: options, getVersion: getVersion, getDir: getDir,
	}, nil
}

func printSteps(result headgatemigrate.Result) {
	if len(result.Steps) == 0 {
		fmt.Println("no-op: already at target version")
		return
	}
	for _, step := range result.Steps {
		dry := ""
		if result.DryRun {
			dry = " dry_run=true"
		}
		fmt.Printf("%s version=%d name=%s online_safe=%t%s\n",
			step.Direction, step.Migration.Version, step.Migration.Name, step.Migration.OnlineSafe, dry)
	}
}

func runPostgres(ctx context.Context, config cli) error {
	conn, err := pgx.Connect(ctx, config.databaseURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	switch config.command {
	case up:
		var result headgatemigrate.Result
		if config.schema == "" {
			result, err = headgatemigrate.MigratePostgres(ctx, conn, headgatemigrate.Up, config.options)
		} else {
			result, err = headgatemigrate.MigratePostgresInSchema(
				ctx, conn, config.schema, headgatemigrate.Up, config.options,
			)
		}
		if err == nil {
			printSteps(result)
		}
		return err
	case down:
		var result headgatemigrate.Result
		if config.schema == "" {
			result, err = headgatemigrate.MigratePostgres(ctx, conn, headgatemigrate.Down, config.options)
		} else {
			result, err = headgatemigrate.MigratePostgresInSchema(
				ctx, conn, config.schema, headgatemigrate.Down, config.options,
			)
		}
		if err == nil {
			printSteps(result)
		}
		return err
	case validate:
		var result headgatemigrate.PostgresValidation
		if config.schema == "" {
			result, err = headgatemigrate.ValidatePostgres(ctx, conn)
		} else {
			result, err = headgatemigrate.ValidatePostgresInSchema(ctx, conn, config.schema)
		}
		if err != nil {
			return err
		}
		if !result.OK() {
			return &headgatemigrate.SchemaError{Messages: result.Messages}
		}
		fmt.Printf("ok backend=postgres current=%d latest=%d\n", result.CurrentVersion, result.LatestVersion)
	case list:
		var applied []headgatemigrate.AppliedMigration
		if config.schema == "" {
			_, applied, err = headgatemigrate.AppliedPostgres(ctx, conn)
		} else {
			_, applied, err = headgatemigrate.AppliedPostgresInSchema(ctx, conn, config.schema)
		}
		if err != nil {
			return err
		}
		printList(headgatemigrate.Postgres, applied)
	case version:
		var applied []headgatemigrate.AppliedMigration
		if config.schema == "" {
			_, applied, err = headgatemigrate.AppliedPostgres(ctx, conn)
		} else {
			_, applied, err = headgatemigrate.AppliedPostgresInSchema(ctx, conn, config.schema)
		}
		if err != nil {
			return err
		}
		printVersion(headgatemigrate.Postgres, applied)
	case adopt:
		var applied []headgatemigrate.AppliedMigration
		if config.schema == "" {
			applied, err = headgatemigrate.AdoptPostgres(ctx, conn)
		} else {
			applied, err = headgatemigrate.AdoptPostgresInSchema(ctx, conn, config.schema)
		}
		if err != nil {
			return err
		}
		fmt.Printf("adopted version=%d\n", currentVersion(applied))
	}
	return nil
}

func mysqlDSN(value string) (string, error) {
	if !strings.HasPrefix(value, "mysql://") {
		return value, nil
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", err
	}
	password, _ := parsed.User.Password()
	database := strings.TrimPrefix(parsed.Path, "/")
	if parsed.User.Username() == "" || parsed.Host == "" || database == "" {
		return "", fmt.Errorf("mysql URL must include user, host, and database")
	}
	query := parsed.Query()
	query.Set("parseTime", "true")
	return fmt.Sprintf("%s:%s@tcp(%s)/%s?%s",
		parsed.User.Username(), password, parsed.Host, database, query.Encode()), nil
}

func runMySQL(ctx context.Context, config cli) error {
	dsn, err := mysqlDSN(config.databaseURL)
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	lockNS := config.lockNS
	if lockNS == "" {
		lockNS = headgatemigrate.DefaultMySQLLockNamespace
	}
	switch config.command {
	case up:
		result, err := headgatemigrate.MigrateMySQLWithLockNamespace(
			ctx, db, headgatemigrate.Up, config.options, lockNS,
		)
		if err == nil {
			printSteps(result)
		}
		return err
	case down:
		result, err := headgatemigrate.MigrateMySQLWithLockNamespace(
			ctx, db, headgatemigrate.Down, config.options, lockNS,
		)
		if err == nil {
			printSteps(result)
		}
		return err
	case validate:
		result, err := headgatemigrate.ValidateMySQL(ctx, db)
		if err != nil {
			return err
		}
		if !result.OK() {
			return &headgatemigrate.SchemaError{Messages: result.Messages}
		}
		fmt.Printf("ok backend=mysql current=%d latest=%d\n", result.CurrentVersion, result.LatestVersion)
	case list:
		_, applied, err := headgatemigrate.AppliedMySQL(ctx, db)
		if err != nil {
			return err
		}
		printList(headgatemigrate.MySQL, applied)
	case version:
		_, applied, err := headgatemigrate.AppliedMySQL(ctx, db)
		if err != nil {
			return err
		}
		printVersion(headgatemigrate.MySQL, applied)
	case adopt:
		applied, err := headgatemigrate.AdoptMySQLWithLockNamespace(ctx, db, lockNS)
		if err != nil {
			return err
		}
		fmt.Printf("adopted version=%d\n", currentVersion(applied))
	}
	return nil
}

func currentVersion(applied []headgatemigrate.AppliedMigration) int {
	if len(applied) == 0 {
		return 0
	}
	return applied[len(applied)-1].Version
}

func printVersion(backend headgatemigrate.Backend, applied []headgatemigrate.AppliedMigration) {
	fmt.Printf("current=%d latest=%d\n", currentVersion(applied), headgatemigrate.LatestVersion(backend))
}

func printList(backend headgatemigrate.Backend, applied []headgatemigrate.AppliedMigration) {
	for _, migration := range headgatemigrate.Migrations(backend) {
		isApplied := false
		for _, row := range applied {
			if row.Version == migration.Version {
				isApplied = true
				break
			}
		}
		fmt.Printf("version=%d name=%s checksum=%s online_safe=%t applied=%t\n",
			migration.Version, migration.Name, headgatemigrate.Checksum(migration), migration.OnlineSafe, isApplied)
	}
}

func main() {
	config, err := parseCLI(os.Args[1:], os.Getenv)
	if err != nil {
		if err.Error() == usage {
			fmt.Println(usage)
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if config.command == get {
		migration, ok := headgatemigrate.GetMigration(config.backend, config.getVersion)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown %s migration version %d\n", config.backend, config.getVersion)
			os.Exit(2)
		}
		if config.getDir == headgatemigrate.Up {
			if config.schema == "" {
				fmt.Print(migration.UpSQL)
			} else {
				namespace, _ := postgressql.NewNamespace(config.schema)
				fmt.Print(namespace.Render(migration.UpSQL))
			}
		} else {
			if config.schema == "" {
				fmt.Print(migration.DownSQL)
			} else {
				namespace, _ := postgressql.NewNamespace(config.schema)
				fmt.Print(namespace.Render(migration.DownSQL))
			}
		}
		return
	}
	ctx := context.Background()
	if config.backend == headgatemigrate.Postgres {
		err = runPostgres(ctx, config)
	} else {
		err = runMySQL(ctx, config)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
