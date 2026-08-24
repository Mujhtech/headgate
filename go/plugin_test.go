package headgate_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	headgate "github.com/mujhtech/headgate"
	"github.com/mujhtech/headgate/headgatetest"
)

func recordingMiddleware(name string, events *[]string) headgate.EnqueueMiddleware {
	return headgate.EnqueueMiddlewareFunc(func(
		ctx context.Context,
		request headgate.EnqueueRequest,
		next headgate.EnqueueNext,
	) error {
		*events = append(*events, name+":before")
		err := next.Run(ctx, request)
		*events = append(*events, name+":after")
		return err
	})
}

func recordingHook(name string, events *[]string) headgate.InsertHook {
	return headgate.InsertHookFunc(func(_ context.Context, event headgate.InsertHookEvent) {
		*events = append(*events, name+":"+string(event.Phase()))
	})
}

func TestPluginsInstallAsOrderedBundlesWithGlobalBeforeScoped(t *testing.T) {
	store := headgatetest.New()
	var events []string
	scoped, err := headgate.NewPlugin(
		"mail-policy",
		headgate.WithPluginKinds("mail.send"),
		headgate.WithPluginEnqueueMiddleware(
			recordingMiddleware("scoped.m1", &events),
			recordingMiddleware("scoped.m2", &events),
		),
		headgate.WithPluginInsertHooks(
			recordingHook("scoped.h1", &events),
			recordingHook("scoped.h2", &events),
		),
	)
	if err != nil {
		t.Fatalf("new scoped plugin: %v", err)
	}
	global, err := headgate.NewPlugin(
		"telemetry",
		headgate.WithPluginEnqueueMiddleware(
			recordingMiddleware("global.m1", &events),
			recordingMiddleware("global.m2", &events),
		),
		headgate.WithPluginInsertHooks(
			recordingHook("global.h1", &events),
			recordingHook("global.h2", &events),
		),
	)
	if err != nil {
		t.Fatalf("new global plugin: %v", err)
	}
	client := headgate.NewClient(
		store,
		// Scoped is deliberately installed first; class order still puts global first.
		headgate.WithPlugins(scoped, global),
		headgate.WithEnqueueMiddleware(recordingMiddleware("standalone.m", &events)),
		headgate.WithInsertHooks(recordingHook("standalone.h", &events)),
	)

	if err := client.Enqueue(context.Background(), []headgate.Envelope{
		authorizationEnvelope("plugin-order", "mail.send"),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	want := []string{
		"standalone.m:before",
		"global.m1:before", "global.m2:before",
		"scoped.m1:before", "scoped.m2:before",
		"standalone.h:begin",
		"global.h1:begin", "global.h2:begin",
		"scoped.h1:begin", "scoped.h2:begin",
		"standalone.h:end",
		"global.h1:end", "global.h2:end",
		"scoped.h1:end", "scoped.h2:end",
		"scoped.m2:after", "scoped.m1:after",
		"global.m2:after", "global.m1:after",
		"standalone.m:after",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
	if _, _, ok := store.JobState("plugin-order"); !ok {
		t.Fatal("plugin-wrapped enqueue did not reach store")
	}
}

func TestScopedPluginSkipsNonmatchesAndNeverSplitsMixedAtomicBatch(t *testing.T) {
	store := headgatetest.New()
	var events []string
	scoped, err := headgate.NewPlugin(
		"mail-policy",
		headgate.WithPluginKinds("mail.send", "mail.send"),
		headgate.WithPluginEnqueueMiddleware(recordingMiddleware("scoped.m", &events)),
		headgate.WithPluginInsertHooks(recordingHook("scoped.h", &events)),
	)
	if err != nil {
		t.Fatalf("new scoped plugin: %v", err)
	}
	if got := scoped.Kinds(); !reflect.DeepEqual(got, []string{"mail.send"}) {
		t.Fatalf("kinds = %#v, want sorted deduplicated scope", got)
	}
	client := headgate.NewClient(store, headgate.WithPlugins(scoped))

	if err := client.Enqueue(context.Background(), []headgate.Envelope{
		authorizationEnvelope("plugin-skip", "image.resize"),
	}); err != nil {
		t.Fatalf("nonmatching enqueue: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("scoped plugin ran for a nonmatch: %#v", events)
	}
	if err := client.Enqueue(context.Background(), []headgate.Envelope{
		authorizationEnvelope("plugin-mixed-image", "image.resize"),
		authorizationEnvelope("plugin-mixed-mail", "mail.send"),
	}); err != nil {
		t.Fatalf("mixed atomic enqueue: %v", err)
	}
	want := []string{"scoped.m:before", "scoped.h:begin", "scoped.h:end", "scoped.m:after"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want one activation around the whole batch", events)
	}
	if _, _, ok := store.JobState("plugin-mixed-image"); !ok {
		t.Fatal("mixed batch was split or image job was lost")
	}
	if _, _, ok := store.JobState("plugin-mixed-mail"); !ok {
		t.Fatal("mixed batch was split or mail job was lost")
	}
}

func TestPluginConfigurationRejectsEmptyIdentityAndInvalidScope(t *testing.T) {
	if _, err := headgate.NewPlugin("   "); !errors.Is(err, headgate.ErrInvalidPlugin) {
		t.Fatalf("blank name error = %v, want ErrInvalidPlugin", err)
	}
	if _, err := headgate.NewPlugin("empty", headgate.WithPluginKinds()); !errors.Is(err, headgate.ErrInvalidPlugin) {
		t.Fatalf("empty scope error = %v, want ErrInvalidPlugin", err)
	}
	if _, err := headgate.NewPlugin("bad", headgate.WithPluginKinds("bad kind")); !errors.Is(err, headgate.ErrInvalidPlugin) {
		t.Fatalf("invalid kind error = %v, want ErrInvalidPlugin", err)
	}
}
