package headgatemigrate

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/mujhtech/headgate/go/postgressql"
)

const createPostgresHistory = `
CREATE TABLE IF NOT EXISTS headgate_schema_migration (
  line          text   NOT NULL DEFAULT 'main',
  version       bigint NOT NULL,
  name          text   NOT NULL,
  checksum      text   NOT NULL,
  applied_at_ms bigint NOT NULL,
  PRIMARY KEY (line, version)
)`

type PostgresValidation struct {
	State          InstallationState
	CurrentVersion int
	LatestVersion  int
	Applied        []AppliedMigration
	Messages       []string
}

func (v PostgresValidation) OK() bool { return len(v.Messages) == 0 }

type pgReader interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func postgresNamespace(
	ctx context.Context,
	conn *pgx.Conn,
	explicitSchema *string,
) (postgressql.Namespace, error) {
	var name string
	if explicitSchema == nil {
		if err := conn.QueryRow(ctx, "SELECT current_schema()").Scan(&name); err != nil {
			return postgressql.Namespace{}, err
		}
	} else {
		name = *explicitSchema
	}
	namespace, err := postgressql.NewNamespace(name)
	if err != nil {
		return postgressql.Namespace{}, err
	}
	var exists bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", name,
	).Scan(&exists); err != nil {
		return postgressql.Namespace{}, err
	}
	if !exists {
		return postgressql.Namespace{}, fmt.Errorf(
			"postgres schema %q does not exist; create it before migrating", name,
		)
	}
	return namespace, nil
}

func pgRelationExists(
	ctx context.Context,
	db pgReader,
	namespace postgressql.Namespace,
	relation string,
) (bool, error) {
	var exists bool
	err := db.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", namespace.Qualified(relation)).Scan(&exists)
	return exists, err
}

func readPostgresHistory(
	ctx context.Context,
	db pgReader,
	namespace postgressql.Namespace,
) ([]AppliedMigration, error) {
	rows, err := db.Query(ctx, namespace.Render(`
SELECT version, name, checksum, applied_at_ms
  FROM headgate_schema_migration
 WHERE line = 'main'
 ORDER BY version`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func appliedPostgresScoped(
	ctx context.Context,
	conn *pgx.Conn,
	namespace postgressql.Namespace,
) (InstallationState, []AppliedMigration, error) {
	hasHistory, err := pgRelationExists(ctx, conn, namespace, "headgate_schema_migration")
	if err != nil {
		return "", nil, err
	}
	hasSchema, err := pgRelationExists(ctx, conn, namespace, "headgate_job")
	if err != nil {
		return "", nil, err
	}
	if !hasHistory {
		if hasSchema {
			return Unversioned, nil, nil
		}
		return Empty, nil, nil
	}
	applied, err := readPostgresHistory(ctx, conn, namespace)
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

func AppliedPostgres(ctx context.Context, conn *pgx.Conn) (InstallationState, []AppliedMigration, error) {
	namespace, err := postgresNamespace(ctx, conn, nil)
	if err != nil {
		return "", nil, err
	}
	return appliedPostgresScoped(ctx, conn, namespace)
}

func AppliedPostgresInSchema(
	ctx context.Context,
	conn *pgx.Conn,
	schema string,
) (InstallationState, []AppliedMigration, error) {
	namespace, err := postgresNamespace(ctx, conn, &schema)
	if err != nil {
		return "", nil, err
	}
	return appliedPostgresScoped(ctx, conn, namespace)
}

func missingPostgresSchema(
	ctx context.Context,
	db pgReader,
	namespace postgressql.Namespace,
) ([]string, error) {
	missing := make([]string, 0)
	tables := map[string]bool{}
	rows, err := db.Query(ctx, `
SELECT table_name
  FROM information_schema.tables
 WHERE table_schema = $1 AND table_name LIKE 'headgate_%'`, namespace.Name())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		tables[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, table := range requiredTables {
		if !tables[table] {
			missing = append(missing, "missing table "+table)
		}
	}

	columns := map[string]bool{}
	rows, err = db.Query(ctx, `
SELECT table_name, column_name
  FROM information_schema.columns
 WHERE table_schema = $1 AND table_name LIKE 'headgate_%'`, namespace.Name())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			rows.Close()
			return nil, err
		}
		columns[table+"."+column] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, column := range postgresJobColumns {
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
	for _, column := range []string{"key", "at_ms"} {
		if !columns["headgate_effect."+column] {
			missing = append(missing, "missing column headgate_effect."+column)
		}
	}

	indexes := map[string]bool{}
	rows, err = db.Query(ctx, `
SELECT indexname FROM pg_indexes
 WHERE schemaname = $1 AND indexname LIKE 'headgate_%'`, namespace.Name())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		indexes[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, index := range postgresIndexes {
		if !indexes[index] {
			missing = append(missing, "missing index "+index)
		}
	}

	triggers := map[string]bool{}
	rows, err = db.Query(ctx, `
SELECT t.tgname
  FROM pg_trigger t
  JOIN pg_class c ON c.oid = t.tgrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = $1 AND NOT t.tgisinternal`, namespace.Name())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		triggers[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, trigger := range postgresTriggers {
		if !triggers[trigger] {
			missing = append(missing, "missing trigger "+trigger)
		}
	}

	rows, err = db.Query(ctx, `
SELECT e.enumlabel
  FROM pg_type t
  JOIN pg_namespace n ON n.oid = t.typnamespace
  JOIN pg_enum e ON e.enumtypid = t.oid
 WHERE n.nspname = $1 AND t.typname = 'headgate_state'
 ORDER BY e.enumsortorder`, namespace.Name())
	if err != nil {
		return nil, err
	}
	states := make([]string, 0)
	for rows.Next() {
		var state string
		if err := rows.Scan(&state); err != nil {
			rows.Close()
			return nil, err
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if !equalStrings(states, stateLabels) {
		missing = append(missing, fmt.Sprintf("headgate_state labels are %v, expected %v", states, stateLabels))
	}
	return missing, nil
}

func validatePostgresScoped(
	ctx context.Context,
	conn *pgx.Conn,
	namespace postgressql.Namespace,
) (PostgresValidation, error) {
	state, applied, err := appliedPostgresScoped(ctx, conn, namespace)
	if err != nil {
		return PostgresValidation{}, err
	}
	current := 0
	if len(applied) != 0 {
		current = applied[len(applied)-1].Version
	}
	latest := LatestVersion(Postgres)
	messages := make([]string, 0)
	switch state {
	case Empty:
		messages = append(messages, "headgate schema is not installed")
	case Unversioned:
		messages = append(messages, "headgate schema exists without migration history")
		shape, err := missingPostgresSchema(ctx, conn, namespace)
		if err != nil {
			return PostgresValidation{}, err
		}
		messages = append(messages, shape...)
	case Versioned:
		if err := ValidateHistory(Postgres, applied); err != nil {
			messages = append(messages, err.Error())
		}
		if current != latest {
			messages = append(messages, fmt.Sprintf("schema is at version %d, embedded latest is %d", current, latest))
		} else {
			shape, err := missingPostgresSchema(ctx, conn, namespace)
			if err != nil {
				return PostgresValidation{}, err
			}
			messages = append(messages, shape...)
		}
	}
	return PostgresValidation{
		State: state, CurrentVersion: current, LatestVersion: latest,
		Applied: applied, Messages: messages,
	}, nil
}

func ValidatePostgres(ctx context.Context, conn *pgx.Conn) (PostgresValidation, error) {
	namespace, err := postgresNamespace(ctx, conn, nil)
	if err != nil {
		return PostgresValidation{}, err
	}
	return validatePostgresScoped(ctx, conn, namespace)
}

func ValidatePostgresInSchema(
	ctx context.Context,
	conn *pgx.Conn,
	schema string,
) (PostgresValidation, error) {
	namespace, err := postgresNamespace(ctx, conn, &schema)
	if err != nil {
		return PostgresValidation{}, err
	}
	return validatePostgresScoped(ctx, conn, namespace)
}

func migratePostgresScoped(
	ctx context.Context,
	conn *pgx.Conn,
	namespace postgressql.Namespace,
	direction Direction,
	options Options,
) (Result, error) {
	state, applied, err := appliedPostgresScoped(ctx, conn, namespace)
	if err != nil {
		return Result{}, err
	}
	if state == Unversioned {
		return Result{}, ErrUnversionedSchema
	}
	planned, err := Plan(Postgres, applied, direction, options)
	if err != nil {
		return Result{}, err
	}
	if options.DryRun {
		return Result{DryRun: true, Steps: planned}, nil
	}
	if _, err := conn.Exec(ctx, namespace.Render(createPostgresHistory)); err != nil {
		return Result{}, err
	}
	executed := make([]Step, 0, len(planned))
	for _, intended := range planned {
		didRun, err := applyPostgresStep(ctx, conn, namespace, direction, intended)
		if err != nil {
			return Result{}, err
		}
		if didRun {
			executed = append(executed, intended)
		}
	}
	return Result{Steps: executed}, nil
}

func MigratePostgres(
	ctx context.Context,
	conn *pgx.Conn,
	direction Direction,
	options Options,
) (Result, error) {
	namespace, err := postgresNamespace(ctx, conn, nil)
	if err != nil {
		return Result{}, err
	}
	return migratePostgresScoped(ctx, conn, namespace, direction, options)
}

func MigratePostgresInSchema(
	ctx context.Context,
	conn *pgx.Conn,
	schema string,
	direction Direction,
	options Options,
) (Result, error) {
	namespace, err := postgresNamespace(ctx, conn, &schema)
	if err != nil {
		return Result{}, err
	}
	return migratePostgresScoped(ctx, conn, namespace, direction, options)
}

func applyPostgresStep(
	ctx context.Context,
	conn *pgx.Conn,
	namespace postgressql.Namespace,
	direction Direction,
	intended Step,
) (bool, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, namespace.Render(
		"LOCK TABLE headgate_schema_migration IN ACCESS EXCLUSIVE MODE",
	)); err != nil {
		return false, err
	}
	live, err := readPostgresHistory(ctx, tx, namespace)
	if err != nil {
		return false, err
	}
	if err := ValidateHistory(Postgres, live); err != nil {
		return false, err
	}
	current := 0
	if len(live) != 0 {
		current = live[len(live)-1].Version
	}
	if direction == Up {
		if current >= intended.Migration.Version {
			return false, tx.Commit(ctx)
		}
		if current+1 != intended.Migration.Version {
			return false, &HistoryError{Message: fmt.Sprintf(
				"concurrent migration moved current version to %d; expected %d",
				current, intended.Migration.Version-1,
			)}
		}
		if err := execPostgresBatch(ctx, tx, namespace.Render(intended.Migration.UpSQL)); err != nil {
			return false, err
		}
		// requiredTables/columns describe latest, not each intermediate version. A
		// fresh install must be allowed to commit v1 before v2 creates its objects.
		if intended.Migration.Version == LatestVersion(Postgres) {
			shape, err := missingPostgresSchema(ctx, tx, namespace)
			if err != nil {
				return false, err
			}
			if len(shape) != 0 {
				return false, &SchemaError{Messages: shape}
			}
		}
		if _, err := tx.Exec(ctx, namespace.Render(`
INSERT INTO headgate_schema_migration (line, version, name, checksum, applied_at_ms)
VALUES ('main', $1, $2, $3,
        (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint)`),
			int64(intended.Migration.Version), intended.Migration.Name, Checksum(intended.Migration)); err != nil {
			return false, err
		}
	} else {
		if current < intended.Migration.Version {
			return false, tx.Commit(ctx)
		}
		if current != intended.Migration.Version {
			return false, &HistoryError{Message: fmt.Sprintf(
				"cannot migrate down version %d; current version is %d", intended.Migration.Version, current,
			)}
		}
		if err := execPostgresBatch(ctx, tx, namespace.Render(intended.Migration.DownSQL)); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, namespace.Render(
			`DELETE FROM headgate_schema_migration WHERE line = 'main' AND version = $1`,
		), int64(intended.Migration.Version)); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func execPostgresBatch(ctx context.Context, tx pgx.Tx, sql string) error {
	_, err := tx.Conn().PgConn().Exec(ctx, sql).ReadAll()
	return err
}

func adoptPostgresScoped(
	ctx context.Context,
	conn *pgx.Conn,
	namespace postgressql.Namespace,
) ([]AppliedMigration, error) {
	state, existing, err := appliedPostgresScoped(ctx, conn, namespace)
	if err != nil {
		return nil, err
	}
	if state == Versioned {
		return existing, ValidateHistory(Postgres, existing)
	}
	if state == Empty {
		return nil, fmt.Errorf("cannot adopt an empty database; migrate up instead")
	}
	if _, err := conn.Exec(ctx, namespace.Render(createPostgresHistory)); err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, namespace.Render(
		"LOCK TABLE headgate_schema_migration IN ACCESS EXCLUSIVE MODE",
	)); err != nil {
		return nil, err
	}
	live, err := readPostgresHistory(ctx, tx, namespace)
	if err != nil {
		return nil, err
	}
	if len(live) != 0 {
		if err := ValidateHistory(Postgres, live); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return live, nil
	}
	shape, err := missingPostgresSchema(ctx, tx, namespace)
	if err != nil {
		return nil, err
	}
	if len(shape) != 0 {
		return nil, &SchemaError{Messages: shape}
	}
	insert := namespace.Render(`
INSERT INTO headgate_schema_migration (line, version, name, checksum, applied_at_ms)
VALUES ('main', $1, $2, $3,
        (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint)`)
	for _, migration := range byBackend[Postgres] {
		if _, err := tx.Exec(ctx, insert,
			int64(migration.Version), migration.Name, Checksum(migration)); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	_, adopted, err := appliedPostgresScoped(ctx, conn, namespace)
	return adopted, err
}

func AdoptPostgres(ctx context.Context, conn *pgx.Conn) ([]AppliedMigration, error) {
	namespace, err := postgresNamespace(ctx, conn, nil)
	if err != nil {
		return nil, err
	}
	return adoptPostgresScoped(ctx, conn, namespace)
}

func AdoptPostgresInSchema(
	ctx context.Context,
	conn *pgx.Conn,
	schema string,
) ([]AppliedMigration, error) {
	namespace, err := postgresNamespace(ctx, conn, &schema)
	if err != nil {
		return nil, err
	}
	return adoptPostgresScoped(ctx, conn, namespace)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
