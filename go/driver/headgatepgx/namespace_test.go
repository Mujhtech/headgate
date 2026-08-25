package headgatepgx

import (
	"strings"
	"testing"
)

func TestExplicitSchemaQuotesObjectsButNotLiteralsCommentsOrAliases(t *testing.T) {
	namespace, err := newPostgresNamespace(`tenant-"blue`)
	if err != nil {
		t.Fatal(err)
	}
	sql := `SELECT headgate_job.id, headgate_inflight_stale FROM headgate_job
		JOIN headgate_rate_bucket b ON true
		WHERE note = 'headgate_job' /* headgate_duty */ -- headgate_worker
		AND state = 'available'::headgate_state`
	rendered := namespace.render(sql)
	for _, expected := range []string{
		`"tenant-""blue".headgate_job.id`, `"tenant-""blue".headgate_rate_bucket`,
		`::"tenant-""blue".headgate_state`, `headgate_inflight_stale FROM`,
		`'headgate_job' /* headgate_duty */`, `-- headgate_worker`,
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("rendered SQL does not contain %q:\n%s", expected, rendered)
		}
	}
}

func TestExplicitSchemaNamespacesNotificationsAndRejectsTruncation(t *testing.T) {
	sql := `SELECT pg_notify('headgate_wakeup', queue) FROM headgate_job`
	if got := (postgresNamespace{}).render(sql); got != sql {
		t.Fatalf("default namespace changed SQL: %q", got)
	}
	namespace, err := newPostgresNamespace("tenant")
	if err != nil {
		t.Fatal(err)
	}
	rendered := namespace.render(sql)
	if !strings.Contains(rendered, namespace.wakeupChannel()) || !strings.Contains(rendered, `"tenant".headgate_job`) {
		t.Fatalf("namespace missing from %q", rendered)
	}
	for _, invalid := range []string{"", "bad\x00schema", strings.Repeat("x", 64)} {
		if _, err := newPostgresNamespace(invalid); err == nil {
			t.Fatalf("schema %q should fail", invalid)
		}
	}
	if _, err := newPostgresNamespace(strings.Repeat("x", 63)); err != nil {
		t.Fatal(err)
	}
}
