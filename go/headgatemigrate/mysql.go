package headgatemigrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const createMySQLHistory = `
CREATE TABLE IF NOT EXISTS headgate_schema_migration (
  line          VARCHAR(64)  NOT NULL DEFAULT 'main',
  version       BIGINT       NOT NULL,
  name          VARCHAR(255) NOT NULL,
  checksum      CHAR(64)     NOT NULL,
  applied_at_ms BIGINT       NOT NULL,
  PRIMARY KEY (line, version)
) ENGINE=InnoDB`

// DefaultMySQLLockNamespace preserves the lock name used before namespaces were
// configurable: headgate:migrate:<database>.
const DefaultMySQLLockNamespace = "headgate"

// MySQLMigrationLockName builds the connection-scoped migration lock key. Readable
// names stay backward-compatible; only an overlong database is hashed under a
// distinct :h: marker to fit MySQL's 64-byte GET_LOCK limit without aliasing a
// short literal database name.
func MySQLMigrationLockName(namespace, database string) (string, error) {
	valid := len(namespace) >= 1 && len(namespace) <= 31
	if valid {
		for index := 0; index < len(namespace); index++ {
			value := namespace[index]
			alphanumeric := value >= 'a' && value <= 'z' ||
				value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
			if !alphanumeric && (index == 0 || value != '_' && value != '-' && value != '.') {
				valid = false
				break
			}
		}
	}
	if !valid {
		return "", fmt.Errorf("MySQL lock namespace must be 1-31 ASCII bytes, start alphanumeric, and contain only [A-Za-z0-9_.-]")
	}
	if database == "" {
		return "", fmt.Errorf("MySQL migrations require a selected database")
	}
	readable := namespace + ":migrate:" + database
	if len(readable) <= 64 {
		return readable, nil
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(database)))
	return namespace + ":h:" + digest[:30], nil
}

type MySQLValidation struct {
	State          InstallationState
	CurrentVersion int
	LatestVersion  int
	Applied        []AppliedMigration
	Messages       []string
}

func (v MySQLValidation) OK() bool { return len(v.Messages) == 0 }

type sqlReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func mysqlTableExists(ctx context.Context, db sqlReader, table string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM information_schema.tables
 WHERE table_schema = DATABASE() AND table_name = ?`, table).Scan(&count)
	return count != 0, err
}

func readMySQLHistory(ctx context.Context, db sqlReader) ([]AppliedMigration, error) {
	rows, err := db.QueryContext(ctx, `
SELECT version, name, checksum, applied_at_ms
  FROM headgate_schema_migration
 WHERE line = 'main'
 ORDER BY version`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]AppliedMigration, 0)
	for rows.Next() {
		var row AppliedMigration
		if err := rows.Scan(&row.Version, &row.Name, &row.Checksum, &row.AppliedAtMS); err != nil {
			return nil, err
		}
		if row.Version < 0 {
			return nil, &HistoryError{Message: fmt.Sprintf("version %d is negative", row.Version)}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func AppliedMySQL(ctx context.Context, db *sql.DB) (InstallationState, []AppliedMigration, error) {
	hasHistory, err := mysqlTableExists(ctx, db, "headgate_schema_migration")
	if err != nil {
		return "", nil, err
	}
	hasSchema, err := mysqlTableExists(ctx, db, "headgate_job")
	if err != nil {
		return "", nil, err
	}
	if !hasHistory {
		if hasSchema {
			return Unversioned, nil, nil
		}
		return Empty, nil, nil
	}
	applied, err := readMySQLHistory(ctx, db)
	if err != nil {
		return "", nil, err
	}
	if len(applied) == 0 {
		if hasSchema {
			return Unversioned, nil, nil
		}
		return Empty, nil, nil
	}
	return Versioned, applied, nil
}

func missingMySQLSchema(ctx context.Context, db sqlReader) ([]string, error) {
	missing := make([]string, 0)
	tables := map[string]bool{}
	rows, err := db.QueryContext(ctx, `
SELECT table_name FROM information_schema.tables
 WHERE table_schema = DATABASE()`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		tables[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Join(err, rows.Close())
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, table := range requiredTables {
		if !tables[table] {
			missing = append(missing, "missing table "+table)
		}
	}

	columns := map[string]bool{}
	rows, err = db.QueryContext(ctx, `
SELECT table_name, column_name FROM information_schema.columns
 WHERE table_schema = DATABASE()`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		columns[table+"."+column] = true
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Join(err, rows.Close())
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, column := range mysqlJobColumns {
		if !columns["headgate_job."+column] {
			missing = append(missing, "missing column headgate_job."+column)
		}
	}
	for table, list := range commonColumns {
		for _, column := range list {
			if !columns[table+"."+column] {
				missing = append(missing, "missing column "+table+"."+column)
			}
		}
	}
	for _, column := range []string{"effect_key", "job_ulid", "claimed_at_ms"} {
		if !columns["headgate_effect."+column] {
			missing = append(missing, "missing column headgate_effect."+column)
		}
	}

	indexes := map[string]bool{}
	rows, err = db.QueryContext(ctx, `
SELECT DISTINCT index_name FROM information_schema.statistics
 WHERE table_schema = DATABASE()`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		indexes[name] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()
	for _, index := range mysqlIndexes {
		if !indexes[index] {
			missing = append(missing, "missing index "+index)
		}
	}

	triggers := map[string]bool{}
	rows, err = db.QueryContext(ctx, `
SELECT trigger_name FROM information_schema.triggers
 WHERE trigger_schema = DATABASE()`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		triggers[name] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()
	for _, trigger := range mysqlTriggers {
		if !triggers[trigger] {
			missing = append(missing, "missing trigger "+trigger)
		}
	}

	var stateType string
	err = db.QueryRowContext(ctx, `
SELECT column_type FROM information_schema.columns
 WHERE table_schema = DATABASE()
   AND table_name = 'headgate_job' AND column_name = 'state'`).Scan(&stateType)
	if err == sql.ErrNoRows {
		stateType = ""
	} else if err != nil {
		return nil, err
	}
	if !equalSets(enumLabels(stateType), stateLabels) {
		missing = append(missing, fmt.Sprintf(
			"headgate_job.state labels are %v, expected %v", enumLabels(stateType), stateLabels,
		))
	}

	var saturationType string
	err = db.QueryRowContext(ctx, `
SELECT column_type FROM information_schema.columns
 WHERE table_schema = DATABASE()
   AND table_name = 'headgate_concurrency_limit' AND column_name = 'on_saturated'`).Scan(&saturationType)
	if err == sql.ErrNoRows {
		saturationType = ""
	} else if err != nil {
		return nil, err
	}
	wantSaturation := []string{"queue", "discard", "cancel_running", "cancel_incoming"}
	if !equalSets(enumLabels(saturationType), wantSaturation) {
		missing = append(missing, fmt.Sprintf(
			"headgate_concurrency_limit.on_saturated labels are %v, expected %v",
			enumLabels(saturationType), wantSaturation,
		))
	}
	return missing, nil
}

func ValidateMySQL(ctx context.Context, db *sql.DB) (MySQLValidation, error) {
	state, applied, err := AppliedMySQL(ctx, db)
	if err != nil {
		return MySQLValidation{}, err
	}
	current := 0
	if len(applied) != 0 {
		current = applied[len(applied)-1].Version
	}
	latest := LatestVersion(MySQL)
	messages := make([]string, 0)
	switch state {
	case Empty:
		messages = append(messages, "headgate schema is not installed")
	case Unversioned:
		messages = append(messages, "headgate schema exists without migration history")
		shape, err := missingMySQLSchema(ctx, db)
		if err != nil {
			return MySQLValidation{}, err
		}
		messages = append(messages, shape...)
	case Versioned:
		if err := ValidateHistory(MySQL, applied); err != nil {
			messages = append(messages, err.Error())
		}
		if current != latest {
			messages = append(messages, fmt.Sprintf("schema is at version %d, embedded latest is %d", current, latest))
		} else {
			shape, err := missingMySQLSchema(ctx, db)
			if err != nil {
				return MySQLValidation{}, err
			}
			messages = append(messages, shape...)
		}
	}
	return MySQLValidation{
		State: state, CurrentVersion: current, LatestVersion: latest,
		Applied: applied, Messages: messages,
	}, nil
}

func MigrateMySQL(ctx context.Context, db *sql.DB, direction Direction, options Options) (Result, error) {
	return MigrateMySQLWithLockNamespace(ctx, db, direction, options, DefaultMySQLLockNamespace)
}

func MigrateMySQLWithLockNamespace(
	ctx context.Context,
	db *sql.DB,
	direction Direction,
	options Options,
	lockNamespace string,
) (Result, error) {
	if _, err := MySQLMigrationLockName(lockNamespace, "validation"); err != nil {
		return Result{}, err
	}
	state, applied, err := AppliedMySQL(ctx, db)
	if err != nil {
		return Result{}, err
	}
	if state == Unversioned {
		return Result{}, ErrUnversionedSchema
	}
	planned, err := Plan(MySQL, applied, direction, options)
	if err != nil {
		return Result{}, err
	}
	if options.DryRun {
		return Result{DryRun: true, Steps: planned}, nil
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, createMySQLHistory); err != nil {
		return Result{}, err
	}
	lockName, err := acquireMySQLLock(ctx, conn, lockNamespace)
	if err != nil {
		return Result{}, err
	}
	result, runErr := migrateMySQLLocked(ctx, conn, direction, options)
	releaseErr := releaseMySQLLock(ctx, conn, lockName)
	if runErr != nil {
		return Result{}, runErr
	}
	if releaseErr != nil {
		return Result{}, releaseErr
	}
	return result, nil
}

func migrateMySQLLocked(ctx context.Context, conn *sql.Conn, direction Direction, options Options) (Result, error) {
	live, err := readMySQLHistory(ctx, conn)
	if err != nil {
		return Result{}, err
	}
	planned, err := Plan(MySQL, live, direction, options)
	if err != nil {
		return Result{}, err
	}
	executed := make([]Step, 0, len(planned))
	for _, step := range planned {
		statements, err := splitMySQLStatements(map[bool]string{true: step.Migration.UpSQL, false: step.Migration.DownSQL}[direction == Up])
		if err != nil {
			return Result{}, err
		}
		for _, statement := range statements {
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				return Result{}, err
			}
		}
		if direction == Up {
			if step.Migration.Version == LatestVersion(MySQL) {
				shape, err := missingMySQLSchema(ctx, conn)
				if err != nil {
					return Result{}, err
				}
				if len(shape) != 0 {
					return Result{}, &SchemaError{Messages: shape}
				}
			}
			if _, err := conn.ExecContext(ctx, `
INSERT INTO headgate_schema_migration (line, version, name, checksum, applied_at_ms)
VALUES ('main', ?, ?, ?, CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED))`,
				step.Migration.Version, step.Migration.Name, Checksum(step.Migration)); err != nil {
				return Result{}, err
			}
		} else {
			if _, err := conn.ExecContext(ctx, `
DELETE FROM headgate_schema_migration WHERE line = 'main' AND version = ?`, step.Migration.Version); err != nil {
				return Result{}, err
			}
		}
		executed = append(executed, step)
	}
	return Result{Steps: executed}, nil
}

func AdoptMySQL(ctx context.Context, db *sql.DB) ([]AppliedMigration, error) {
	return AdoptMySQLWithLockNamespace(ctx, db, DefaultMySQLLockNamespace)
}

func AdoptMySQLWithLockNamespace(
	ctx context.Context,
	db *sql.DB,
	lockNamespace string,
) ([]AppliedMigration, error) {
	if _, err := MySQLMigrationLockName(lockNamespace, "validation"); err != nil {
		return nil, err
	}
	state, existing, err := AppliedMySQL(ctx, db)
	if err != nil {
		return nil, err
	}
	if state == Versioned {
		return existing, ValidateHistory(MySQL, existing)
	}
	if state == Empty {
		return nil, fmt.Errorf("cannot adopt an empty database; migrate up instead")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, createMySQLHistory); err != nil {
		return nil, err
	}
	lockName, err := acquireMySQLLock(ctx, conn, lockNamespace)
	if err != nil {
		return nil, err
	}
	result, runErr := adoptMySQLLocked(ctx, conn)
	releaseErr := releaseMySQLLock(ctx, conn, lockName)
	if runErr != nil {
		return nil, runErr
	}
	if releaseErr != nil {
		return nil, releaseErr
	}
	return result, nil
}

func adoptMySQLLocked(ctx context.Context, conn *sql.Conn) ([]AppliedMigration, error) {
	live, err := readMySQLHistory(ctx, conn)
	if err != nil {
		return nil, err
	}
	if len(live) != 0 {
		return live, ValidateHistory(MySQL, live)
	}
	shape, err := missingMySQLSchema(ctx, conn)
	if err != nil {
		return nil, err
	}
	if len(shape) != 0 {
		return nil, &SchemaError{Messages: shape}
	}
	for _, migration := range byBackend[MySQL] {
		if _, err := conn.ExecContext(ctx, `
INSERT INTO headgate_schema_migration (line, version, name, checksum, applied_at_ms)
VALUES ('main', ?, ?, ?, CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED))`,
			migration.Version, migration.Name, Checksum(migration)); err != nil {
			return nil, err
		}
	}
	return readMySQLHistory(ctx, conn)
}

func acquireMySQLLock(ctx context.Context, conn *sql.Conn, namespace string) (string, error) {
	var database sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database); err != nil {
		return "", err
	}
	name, err := MySQLMigrationLockName(namespace, database.String)
	if err != nil {
		return "", err
	}
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", name).Scan(&acquired); err != nil {
		return "", err
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return "", fmt.Errorf("timed out acquiring the MySQL migration lock")
	}
	return name, nil
}

func releaseMySQLLock(ctx context.Context, conn *sql.Conn, name string) error {
	var released sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", name).Scan(&released); err != nil {
		return err
	}
	if !released.Valid || released.Int64 != 1 {
		return fmt.Errorf("MySQL migration lock was not held at release")
	}
	return nil
}

func enumLabels(columnType string) []string {
	body := strings.TrimSuffix(strings.TrimPrefix(columnType, "enum("), ")")
	if body == columnType {
		return nil
	}
	parts := strings.Split(body, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		result = append(result, strings.ReplaceAll(strings.Trim(strings.TrimSpace(part), "'"), "''", "'"))
	}
	return result
}

func equalSets(a, b []string) bool {
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	return equalStrings(a, b)
}

func splitMySQLStatements(sqlText string) ([]string, error) {
	type mode uint8
	const (
		normal mode = iota
		single
		double
		backtick
		lineComment
		blockComment
	)
	state := normal
	var statement strings.Builder
	statements := make([]string, 0)
	for index := 0; index < len(sqlText); index++ {
		ch := sqlText[index]
		var next byte
		if index+1 < len(sqlText) {
			next = sqlText[index+1]
		}
		switch {
		case state == normal && ch == '-' && next == '-':
			state = lineComment
			index++
		case state == normal && ch == '/' && next == '*':
			state = blockComment
			index++
		case state == lineComment && ch == '\n':
			state = normal
			statement.WriteByte('\n')
		case state == lineComment:
		case state == blockComment && ch == '*' && next == '/':
			state = normal
			index++
		case state == blockComment:
		case state == normal && ch == '\'':
			state = single
			statement.WriteByte(ch)
		case state == normal && ch == '"':
			state = double
			statement.WriteByte(ch)
		case state == normal && ch == '`':
			state = backtick
			statement.WriteByte(ch)
		case state == single || state == double || state == backtick:
			statement.WriteByte(ch)
			quote := byte('\'')
			switch state {
			case double:
				quote = '"'
			case backtick:
				quote = '`'
			}
			if ch == '\\' && index+1 < len(sqlText) {
				index++
				statement.WriteByte(sqlText[index])
			} else if ch == quote {
				if next == quote {
					index++
					statement.WriteByte(next)
				} else {
					state = normal
				}
			}
		case state == normal && ch == ';':
			if value := strings.TrimSpace(statement.String()); value != "" {
				statements = append(statements, value)
			}
			statement.Reset()
		default:
			statement.WriteByte(ch)
		}
	}
	if state == single || state == double || state == backtick || state == blockComment {
		return nil, fmt.Errorf("unterminated quote or block comment in embedded MySQL migration")
	}
	if value := strings.TrimSpace(statement.String()); value != "" {
		statements = append(statements, value)
	}
	return statements, nil
}
