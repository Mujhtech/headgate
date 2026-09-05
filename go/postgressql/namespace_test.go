package postgressql

import (
	"strings"
	"testing"
)

func TestNamespaceQuotesObjectsWithoutTouchingSQLData(t *testing.T) {
	namespace, err := NewNamespace(`tenant-"blue`)
	if err != nil {
		t.Fatal(err)
	}
	sql := `SELECT headgate_job.id, headgate_inflight_stale FROM headgate_job
		JOIN headgate_rate_bucket b ON true
		WHERE note = 'headgate_job' /* headgate_duty */ -- headgate_worker
		AND state = 'available'::headgate_state`
	rendered := namespace.Render(sql)
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

func TestNamespaceOwnsWakeupsAndRejectsTruncation(t *testing.T) {
	namespace, err := NewNamespace("tenant")
	if err != nil {
		t.Fatal(err)
	}
	rendered := namespace.Render(`SELECT pg_notify('headgate_wakeup', queue) FROM headgate_job`)
	if !strings.Contains(rendered, namespace.WakeupChannel()) || !strings.Contains(rendered, `"tenant".headgate_job`) {
		t.Fatalf("namespace missing from %q", rendered)
	}
	for _, invalid := range []string{"", "bad\x00schema", strings.Repeat("x", 64)} {
		if _, err := NewNamespace(invalid); err == nil {
			t.Fatalf("schema %q should fail", invalid)
		}
	}
}

func TestNamespaceOwnsEnqueueBackpressureObjectsOutsideTriggerBody(t *testing.T) {
	namespace, err := NewNamespace("tenant")
	if err != nil {
		t.Fatal(err)
	}
	sql := `CREATE TABLE headgate_enqueue_policy (queue text);
		CREATE TABLE headgate_enqueue_counter (queue text);
		CREATE OR REPLACE FUNCTION headgate_track_enqueue_depth()
		RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		  EXECUTE format('INSERT INTO %I.headgate_enqueue_counter VALUES ($1)',
		                 TG_TABLE_SCHEMA) USING NEW.queue;
		  RETURN NEW;
		END;
		$$;
		CREATE TRIGGER track AFTER INSERT ON headgate_job
		FOR EACH ROW EXECUTE FUNCTION headgate_track_enqueue_depth();`
	rendered := namespace.Render(sql)
	for _, expected := range []string{
		`CREATE TABLE "tenant".headgate_enqueue_policy`,
		`CREATE TABLE "tenant".headgate_enqueue_counter`,
		`CREATE OR REPLACE FUNCTION "tenant".headgate_track_enqueue_depth()`,
		`ON "tenant".headgate_job`,
		`EXECUTE FUNCTION "tenant".headgate_track_enqueue_depth()`,
		`%I.headgate_enqueue_counter`,
		`TG_TABLE_SCHEMA`,
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("rendered SQL does not contain %q:\n%s", expected, rendered)
		}
	}
}

func TestMigrationIndexesAndNewMetricTablesStayInsideExplicitSchema(t *testing.T) {
	namespace, err := NewNamespace("tenant")
	if err != nil {
		t.Fatal(err)
	}
	rendered := namespace.Render(`DROP INDEX headgate_job_unique;
		CREATE UNIQUE INDEX headgate_job_unique ON headgate_job (unique_key);
		CREATE TABLE headgate_job_tag (job_id bigint);
		CREATE TABLE headgate_queue_sample (queue text);
		CREATE TABLE headgate_durable_event_scope (scope text);
		CREATE TABLE headgate_durable_event (scope text REFERENCES headgate_durable_event_scope(scope));
		CREATE INDEX headgate_durable_event_recent ON headgate_durable_event (scope);
		DROP INDEX headgate_durable_event_recent;`)
	for _, expected := range []string{
		`DROP INDEX "tenant".headgate_job_unique`,
		`CREATE UNIQUE INDEX headgate_job_unique ON "tenant".headgate_job`,
		`CREATE TABLE "tenant".headgate_job_tag`,
		`CREATE TABLE "tenant".headgate_queue_sample`,
		`CREATE TABLE "tenant".headgate_durable_event_scope`,
		`CREATE TABLE "tenant".headgate_durable_event`,
		`REFERENCES "tenant".headgate_durable_event_scope`,
		`CREATE INDEX headgate_durable_event_recent ON "tenant".headgate_durable_event`,
		`DROP INDEX "tenant".headgate_durable_event_recent`,
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("rendered SQL does not contain %q:\n%s", expected, rendered)
		}
	}
}
