package headgatepgx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mujhtech/headgate/postgressql"
)

// postgresNamespace is a narrow driver adapter around the dependency-free renderer
// shared with the migration module. Keeping one renderer prevents a store and migrator
// from disagreeing about which durable objects belong to an instance.
type postgresNamespace struct {
	value postgressql.Namespace
}

func newPostgresNamespace(name string) (postgresNamespace, error) {
	namespace, err := postgressql.NewNamespace(name)
	if err != nil {
		return postgresNamespace{}, err
	}
	return postgresNamespace{value: namespace}, nil
}

func (n postgresNamespace) render(sql string) string       { return n.value.Render(sql) }
func (n postgresNamespace) name() string                   { return n.value.Name() }
func (n postgresNamespace) wakeupChannel() string          { return n.value.WakeupChannel() }
func (n postgresNamespace) qualified(object string) string { return n.value.Qualified(object) }

// schemaPool is the one query boundary for internal pool use. Tests intentionally use
// it too, which prevents an inspection or duty query from escaping the namespace while
// the hot admission query happens to be correct.
type schemaPool struct {
	raw       *pgxpool.Pool
	namespace postgresNamespace
}

func (p *schemaPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.raw.Exec(ctx, p.namespace.render(sql), args...)
}

func (p *schemaPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.raw.Query(ctx, p.namespace.render(sql), args...)
}

func (p *schemaPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.raw.QueryRow(ctx, p.namespace.render(sql), args...)
}

func (p *schemaPool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.raw.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &schemaTx{Tx: tx, namespace: p.namespace}, nil
}

type schemaTx struct {
	pgx.Tx
	namespace postgresNamespace
}

func (t *schemaTx) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := t.Tx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &schemaTx{Tx: tx, namespace: t.namespace}, nil
}

func (t *schemaTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.Tx.Exec(ctx, t.namespace.render(sql), args...)
}

func (t *schemaTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.Tx.Query(ctx, t.namespace.render(sql), args...)
}

func (t *schemaTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.Tx.QueryRow(ctx, t.namespace.render(sql), args...)
}

func (t *schemaTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return t.Tx.Prepare(ctx, name, t.namespace.render(sql))
}

type schemaQuerier struct {
	querier
	namespace postgresNamespace
}

func (q schemaQuerier) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return q.querier.Exec(ctx, q.namespace.render(sql), args...)
}

func (q schemaQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return q.querier.Query(ctx, q.namespace.render(sql), args...)
}

func (q schemaQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return q.querier.QueryRow(ctx, q.namespace.render(sql), args...)
}

func (p *schemaPool) scope(q querier) querier {
	switch q.(type) {
	case *schemaPool, *schemaTx, schemaQuerier:
		return q
	default:
		return schemaQuerier{querier: q, namespace: p.namespace}
	}
}
