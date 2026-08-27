package headgatetest

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/mujhtech/headgate/go/headgatemigrate"
	"github.com/redis/go-redis/v9"
)

var nextNamespace atomic.Uint64

func uniqueName(backend string) string {
	return fmt.Sprintf("hg_test_%s_%d_%d", backend, os.Getpid(), nextNamespace.Add(1))
}

// PostgresTestDatabase is one fully migrated, process-unique schema. Config carries the
// schema in the startup search_path and can be passed to pgxpool or pgx.ConnectConfig.
type PostgresTestDatabase struct {
	Schema string
	Config *pgx.ConnConfig

	adminConfig *pgx.ConnConfig
	cleanupOnce sync.Once
	cleanupErr  error
}

func CreatePostgresTestDatabase(ctx context.Context, conninfo string) (*PostgresTestDatabase, error) {
	adminConfig, err := pgx.ParseConfig(conninfo)
	if err != nil {
		return nil, fmt.Errorf("headgatetest: bad Postgres conninfo: %w", err)
	}
	admin, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		return nil, fmt.Errorf("headgatetest: Postgres connect: %w", err)
	}
	defer admin.Close(ctx)
	schema := uniqueName("pg")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		return nil, fmt.Errorf("headgatetest: create schema %s: %w", schema, err)
	}
	testConfig := adminConfig.Copy()
	if testConfig.RuntimeParams == nil {
		testConfig.RuntimeParams = map[string]string{}
	}
	testConfig.RuntimeParams["search_path"] = schema
	_, migrationErr := headgatemigrate.MigratePostgresInSchema(
		ctx, admin, schema, headgatemigrate.Up, headgatemigrate.Options{},
	)
	if migrationErr != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		return nil, fmt.Errorf("headgatetest: migrate Postgres schema: %w", migrationErr)
	}
	return &PostgresTestDatabase{
		Schema: schema, Config: testConfig, adminConfig: adminConfig,
	}, nil
}

func (d *PostgresTestDatabase) Connect(ctx context.Context) (*pgx.Conn, error) {
	return pgx.ConnectConfig(ctx, d.Config.Copy())
}

func (d *PostgresTestDatabase) Cleanup(ctx context.Context) error {
	d.cleanupOnce.Do(func() {
		admin, err := pgx.ConnectConfig(ctx, d.adminConfig)
		if err != nil {
			d.cleanupErr = err
			return
		}
		defer admin.Close(ctx)
		_, d.cleanupErr = admin.Exec(ctx, "DROP SCHEMA "+d.Schema+" CASCADE")
	})
	return d.cleanupErr
}

func RequirePostgresTestDatabase(t testing.TB, ctx context.Context, conninfo string) *PostgresTestDatabase {
	t.Helper()
	database, err := CreatePostgresTestDatabase(ctx, conninfo)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Cleanup(context.Background()); err != nil {
			t.Errorf("cleanup Postgres test database: %v", err)
		}
	})
	return database
}

// MySQLTestDatabase is one fully migrated, process-unique database. DSN is ready for
// database/sql and includes parseTime; credentials are not logged by the helper.
type MySQLTestDatabase struct {
	Database string
	DSN      string

	adminDSN    string
	cleanupOnce sync.Once
	cleanupErr  error
}

func mysqlConfig(value, database string) (*mysqlDriver.Config, error) {
	if !strings.HasPrefix(value, "mysql://") {
		config, err := mysqlDriver.ParseDSN(value)
		if err != nil {
			return nil, err
		}
		if database != "" {
			config.DBName = database
		}
		config.ParseTime = true
		return config, nil
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return nil, err
	}
	password, _ := parsed.User.Password()
	if database == "" {
		database = strings.TrimPrefix(parsed.Path, "/")
	}
	if parsed.User.Username() == "" || parsed.Host == "" || database == "" {
		return nil, fmt.Errorf("MySQL URL must include user, host, and database")
	}
	return &mysqlDriver.Config{
		User: parsed.User.Username(), Passwd: password, Net: "tcp", Addr: parsed.Host,
		DBName: database, ParseTime: true,
	}, nil
}

func CreateMySQLTestDatabase(ctx context.Context, value string) (*MySQLTestDatabase, error) {
	adminConfig, err := mysqlConfig(value, "")
	if err != nil {
		return nil, fmt.Errorf("headgatetest: bad MySQL URL/DSN: %w", err)
	}
	admin, err := sql.Open("mysql", adminConfig.FormatDSN())
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	database := uniqueName("mysql")
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+database); err != nil {
		return nil, fmt.Errorf("headgatetest: create database %s: %w", database, err)
	}
	testConfig := adminConfig.Clone()
	testConfig.DBName = database
	testDB, err := sql.Open("mysql", testConfig.FormatDSN())
	if err != nil {
		_, _ = admin.ExecContext(ctx, "DROP DATABASE "+database)
		return nil, err
	}
	_, migrationErr := headgatemigrate.MigrateMySQL(ctx, testDB, headgatemigrate.Up, headgatemigrate.Options{})
	_ = testDB.Close()
	if migrationErr != nil {
		_, _ = admin.ExecContext(ctx, "DROP DATABASE "+database)
		return nil, fmt.Errorf("headgatetest: migrate MySQL database: %w", migrationErr)
	}
	return &MySQLTestDatabase{
		Database: database, DSN: testConfig.FormatDSN(), adminDSN: adminConfig.FormatDSN(),
	}, nil
}

func (d *MySQLTestDatabase) Open() (*sql.DB, error) { return sql.Open("mysql", d.DSN) }

func (d *MySQLTestDatabase) Cleanup(ctx context.Context) error {
	d.cleanupOnce.Do(func() {
		admin, err := sql.Open("mysql", d.adminDSN)
		if err != nil {
			d.cleanupErr = err
			return
		}
		defer admin.Close()
		_, d.cleanupErr = admin.ExecContext(ctx, "DROP DATABASE "+d.Database)
	})
	return d.cleanupErr
}

func RequireMySQLTestDatabase(t testing.TB, ctx context.Context, value string) *MySQLTestDatabase {
	t.Helper()
	database, err := CreateMySQLTestDatabase(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Cleanup(context.Background()); err != nil {
			t.Errorf("cleanup MySQL test database: %v", err)
		}
	})
	return database
}

// RedisTestNamespace owns one process-unique prefix and one client. Cleanup uses SCAN +
// bounded DEL batches and never flushes another test's keys.
type RedisTestNamespace struct {
	Prefix string
	Client *redis.Client

	cleanupOnce sync.Once
	cleanupErr  error
}

func CreateRedisTestNamespace(ctx context.Context, value string) (*RedisTestNamespace, error) {
	options, err := redis.ParseURL(value)
	if err != nil {
		return nil, fmt.Errorf("headgatetest: bad Redis URL: %w", err)
	}
	namespace := &RedisTestNamespace{Prefix: uniqueName("redis"), Client: redis.NewClient(options)}
	keys, err := namespace.keys(ctx)
	if err != nil {
		_ = namespace.Client.Close()
		return nil, err
	}
	if len(keys) != 0 {
		_ = namespace.Client.Close()
		return nil, fmt.Errorf("headgatetest: generated Redis prefix %s already exists", namespace.Prefix)
	}
	return namespace, nil
}

func (n *RedisTestNamespace) keys(ctx context.Context) ([]string, error) {
	var cursor uint64
	var keys []string
	for {
		page, next, err := n.Client.Scan(ctx, cursor, n.Prefix+":*", 100).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, page...)
		cursor = next
		if cursor == 0 {
			return keys, nil
		}
	}
}

func (n *RedisTestNamespace) Cleanup(ctx context.Context) error {
	n.cleanupOnce.Do(func() {
		keys, err := n.keys(ctx)
		if err != nil {
			n.cleanupErr = err
			return
		}
		for len(keys) != 0 {
			count := 100
			if len(keys) < count {
				count = len(keys)
			}
			if err := n.Client.Del(ctx, keys[:count]...).Err(); err != nil {
				n.cleanupErr = err
				return
			}
			keys = keys[count:]
		}
		n.cleanupErr = n.Client.Close()
	})
	return n.cleanupErr
}

func RequireRedisTestNamespace(t testing.TB, ctx context.Context, value string) *RedisTestNamespace {
	t.Helper()
	namespace, err := CreateRedisTestNamespace(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := namespace.Cleanup(context.Background()); err != nil {
			t.Errorf("cleanup Redis test namespace: %v", err)
		}
	})
	return namespace
}
