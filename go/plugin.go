package headgate

import (
	"context"
	"errors"
	"sort"
	"strings"
)

// ErrInvalidPlugin is returned when a plugin has no useful identity or an invalid kind
// scope. Components themselves use the existing middleware and hook contracts.
var ErrInvalidPlugin = errors.New("headgate: invalid plugin")

// PluginConfigError identifies the invalid plugin field while preserving a stable
// sentinel for errors.Is.
type PluginConfigError struct {
	Field string
	Msg   string
}

func (e *PluginConfigError) Error() string {
	return "headgate: invalid plugin " + e.Field + ": " + e.Msg
}

func (e *PluginConfigError) Unwrap() error { return ErrInvalidPlugin }

// Plugin is an installable bundle of enqueue middleware and insert hooks. An empty kind
// set means global; a scoped plugin activates when any envelope in an atomic batch
// matches and then observes the whole batch.
type Plugin struct {
	name        string
	kinds       map[string]struct{}
	middlewares []EnqueueMiddleware
	hooks       []InsertHook
}

// PluginOption configures a Plugin while allowing validation to remain at construction.
type PluginOption func(*Plugin) error

// NewPlugin constructs a global plugin unless WithPluginKinds is supplied.
func NewPlugin(name string, options ...PluginOption) (Plugin, error) {
	if strings.TrimSpace(name) == "" {
		return Plugin{}, &PluginConfigError{Field: "name", Msg: "must not be empty"}
	}
	plugin := Plugin{name: name}
	for _, option := range options {
		if err := option(&plugin); err != nil {
			return Plugin{}, err
		}
	}
	return plugin, nil
}

// WithPluginKinds scopes the plugin to a sorted-set-equivalent collection of task kinds.
// At least one kind is required; duplicate kinds are harmless.
func WithPluginKinds(kinds ...string) PluginOption {
	owned := append([]string(nil), kinds...)
	return func(plugin *Plugin) error {
		if len(owned) == 0 {
			return &PluginConfigError{Field: "kinds", Msg: "must name at least one kind"}
		}
		scope := make(map[string]struct{}, len(owned))
		for _, kind := range owned {
			if err := ValidateKind(kind); err != nil {
				return &PluginConfigError{Field: "kinds", Msg: err.Error()}
			}
			scope[kind] = struct{}{}
		}
		plugin.kinds = scope
		return nil
	}
}

// WithPluginEnqueueMiddleware appends middleware inside the plugin's contiguous wrapper.
func WithPluginEnqueueMiddleware(middlewares ...EnqueueMiddleware) PluginOption {
	owned := append([]EnqueueMiddleware(nil), middlewares...)
	return func(plugin *Plugin) error {
		plugin.middlewares = append(plugin.middlewares, owned...)
		return nil
	}
}

// WithPluginInsertHooks appends sequential insert observers inside the plugin bundle.
func WithPluginInsertHooks(hooks ...InsertHook) PluginOption {
	owned := append([]InsertHook(nil), hooks...)
	return func(plugin *Plugin) error {
		plugin.hooks = append(plugin.hooks, owned...)
		return nil
	}
}

func (p Plugin) Name() string { return p.name }

// Kinds returns nil for a global plugin and an owned, deterministically sorted slice for
// a scoped plugin.
func (p Plugin) Kinds() []string {
	if p.kinds == nil {
		return nil
	}
	kinds := make([]string, 0, len(p.kinds))
	for kind := range p.kinds {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

func (p Plugin) isGlobal() bool { return p.kinds == nil }

func (p Plugin) matches(batch []Envelope) bool {
	if p.isGlobal() {
		return true
	}
	for _, envelope := range batch {
		if _, ok := p.kinds[envelope.Kind]; ok {
			return true
		}
	}
	return false
}

type pluginMiddlewareGroup struct {
	plugin Plugin
}

func (g pluginMiddlewareGroup) HandleEnqueue(
	ctx context.Context,
	request EnqueueRequest,
	next EnqueueNext,
) error {
	if !g.plugin.matches(request.Batch) {
		return next.Run(ctx, request)
	}
	return newEnqueueNext(g.plugin.middlewares, next.Run).Run(ctx, request)
}

type pluginHookGroup struct {
	plugin Plugin
}

func (g pluginHookGroup) OnInsert(ctx context.Context, event InsertHookEvent) {
	if !g.plugin.matches(event.attempt.batch) {
		return
	}
	for _, hook := range g.plugin.hooks {
		hook.OnInsert(ctx, event)
	}
}
