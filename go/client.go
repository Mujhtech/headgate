package headgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// EnqueueSource tells an authorizer whether a decision came from an internal library
// client or the HTTP API. Policies never need to infer that distinction from headers.
type EnqueueSource string

const (
	EnqueueSourceLibrary EnqueueSource = "library"
	EnqueueSourceHTTP    EnqueueSource = "http"
)

// EnqueueIdentity is established by the embedding application before headgate sees a
// request. Attributes are deliberately application-defined: the queue does not invent a
// role model. HTTP middleware attaches this value after authentication; headgate never
// trusts a caller-controlled identity header.
type EnqueueIdentity struct {
	Subject    string
	Attributes map[string]string
}

type enqueueIdentityKey struct{}

// WithEnqueueIdentity attaches an authenticated identity for the library client or HTTP
// authorizer. The value is copied so later caller mutation cannot change an in-flight
// decision.
func WithEnqueueIdentity(ctx context.Context, identity EnqueueIdentity) context.Context {
	copyIdentity := identity
	copyIdentity.Attributes = cloneStringMap(identity.Attributes)
	return context.WithValue(ctx, enqueueIdentityKey{}, copyIdentity)
}

// EnqueueIdentityFromContext returns the identity installed by trusted application
// middleware. ok=false means anonymous; the configured policy decides whether that is
// allowed.
func EnqueueIdentityFromContext(ctx context.Context) (EnqueueIdentity, bool) {
	identity, ok := ctx.Value(enqueueIdentityKey{}).(EnqueueIdentity)
	if !ok {
		return EnqueueIdentity{}, false
	}
	identity.Attributes = cloneStringMap(identity.Attributes)
	return identity, true
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// EnqueueAuthorization is supplied to every per-envelope authorization decision.
type EnqueueAuthorization struct {
	Source   EnqueueSource
	Identity *EnqueueIdentity
}

// EnqueueAuthorizer is application policy. Returning false rejects the entire batch
// before any store call.
type EnqueueAuthorizer interface {
	AuthorizeEnqueue(context.Context, EnqueueAuthorization, Envelope) bool
}

// EnqueueAuthorizeFunc adapts a function into an EnqueueAuthorizer.
type EnqueueAuthorizeFunc func(context.Context, EnqueueAuthorization, Envelope) bool

func (f EnqueueAuthorizeFunc) AuthorizeEnqueue(
	ctx context.Context,
	authorization EnqueueAuthorization,
	envelope Envelope,
) bool {
	return f(ctx, authorization, envelope)
}

// AllowAllEnqueues is the backward-compatible default. Authentication and identity
// remain the embedding application's responsibility.
type AllowAllEnqueues struct{}

func (AllowAllEnqueues) AuthorizeEnqueue(context.Context, EnqueueAuthorization, Envelope) bool {
	return true
}

var ErrEnqueueForbidden = errors.New("headgate: enqueue forbidden")

// EnqueueForbiddenError is a typed policy rejection. It is neither a store outage nor a
// job failure.
type EnqueueForbiddenError struct {
	Kind string
}

func (e *EnqueueForbiddenError) Error() string {
	return fmt.Sprintf("headgate: enqueue forbidden for kind `%s`", e.Kind)
}

func (e *EnqueueForbiddenError) Unwrap() error { return ErrEnqueueForbidden }

// AuthorizeEnqueueBatch performs the complete authorization pass before any I/O. A nil
// authorizer means the documented allow-all default.
func AuthorizeEnqueueBatch(
	ctx context.Context,
	authorizer EnqueueAuthorizer,
	source EnqueueSource,
	batch []Envelope,
) error {
	if authorizer == nil {
		authorizer = AllowAllEnqueues{}
	}
	var identity *EnqueueIdentity
	if value, ok := EnqueueIdentityFromContext(ctx); ok {
		identity = &value
	}
	authorization := EnqueueAuthorization{Source: source, Identity: identity}
	for _, envelope := range batch {
		if !authorizer.AuthorizeEnqueue(ctx, authorization, envelope) {
			return &EnqueueForbiddenError{Kind: envelope.Kind}
		}
	}
	return nil
}

// ClientOption configures the producer client.
type ClientOption func(*Client)

// WithEnqueueAuthorizer installs application policy. The same policy guards ordinary,
// bulk, and transactional enqueue.
func WithEnqueueAuthorizer(authorizer EnqueueAuthorizer) ClientOption {
	return func(client *Client) { client.authorizer = authorizer }
}

// Client is the producer-facing enqueue boundary. Raw Store remains a trusted low-level
// port for workers and adapters; applications accepting untrusted input should expose a
// Client instead.
type Client struct {
	store                  Store
	authorizer             EnqueueAuthorizer
	breaker                *CircuitBreaker
	middlewares            []EnqueueMiddleware
	insertHooks            []InsertHook
	globalPluginMiddleware []EnqueueMiddleware
	scopedPluginMiddleware []EnqueueMiddleware
	globalPluginHooks      []InsertHook
	scopedPluginHooks      []InsertHook
	eventBus               *EventBus
}

// Completion is the durable terminal state returned by EnqueueAndWait. Result is nil
// for jobs that completed without bytes and for non-success terminal states.
type Completion struct {
	JobID  string
	State  string
	Result *JobResult
	Error  string
}

var ErrWaitUnsupported = errors.New("headgate: insert-and-await is unsupported")

// WithEventBus installs the same process-local bus configured on the worker. Events
// provide latency; durable Inspect reads provide correctness.
func WithEventBus(eventBus *EventBus) ClientOption {
	return func(client *Client) { client.eventBus = eventBus }
}

// WithCircuitBreaker installs a local availability circuit. Sharing one breaker across
// clients gives one process a coherent outage view; nil disables it.
func WithCircuitBreaker(breaker *CircuitBreaker) ClientOption {
	return func(client *Client) { client.breaker = breaker }
}

// WithEnqueueMiddleware appends ordered producer middleware. Registration order is
// nesting order: the first middleware runs its before half first and after half last.
func WithEnqueueMiddleware(middlewares ...EnqueueMiddleware) ClientOption {
	return func(client *Client) {
		client.middlewares = append(client.middlewares, middlewares...)
	}
}

// WithInsertHooks appends non-wrapping observers of every actual enqueue store attempt.
func WithInsertHooks(hooks ...InsertHook) ClientOption {
	return func(client *Client) {
		client.insertHooks = append(client.insertHooks, hooks...)
	}
}

// WithPlugins installs producer bundles. Standalone components always run first,
// followed by global plugins and then matching scoped plugins. Install order is stable
// within each plugin class, even when global and scoped plugins are supplied interleaved.
func WithPlugins(plugins ...Plugin) ClientOption {
	owned := append([]Plugin(nil), plugins...)
	return func(client *Client) {
		for _, plugin := range owned {
			plugin.middlewares = append([]EnqueueMiddleware(nil), plugin.middlewares...)
			plugin.hooks = append([]InsertHook(nil), plugin.hooks...)
			if plugin.kinds != nil {
				plugin.kinds = cloneKindSet(plugin.kinds)
			}
			if len(plugin.middlewares) != 0 {
				group := pluginMiddlewareGroup{plugin: plugin}
				if plugin.isGlobal() {
					client.globalPluginMiddleware = append(client.globalPluginMiddleware, group)
				} else {
					client.scopedPluginMiddleware = append(client.scopedPluginMiddleware, group)
				}
			}
			if len(plugin.hooks) != 0 {
				group := pluginHookGroup{plugin: plugin}
				if plugin.isGlobal() {
					client.globalPluginHooks = append(client.globalPluginHooks, group)
				} else {
					client.scopedPluginHooks = append(client.scopedPluginHooks, group)
				}
			}
		}
	}
}

func cloneKindSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for kind := range in {
		out[kind] = struct{}{}
	}
	return out
}

func (c *Client) middlewareChain() []EnqueueMiddleware {
	chain := make([]EnqueueMiddleware, 0,
		len(c.middlewares)+len(c.globalPluginMiddleware)+len(c.scopedPluginMiddleware))
	chain = append(chain, c.middlewares...)
	chain = append(chain, c.globalPluginMiddleware...)
	chain = append(chain, c.scopedPluginMiddleware...)
	return chain
}

// NewClient preserves existing behavior with an allow-all default. Installing an
// authorizer is explicit.
func NewClient(store Store, options ...ClientOption) *Client {
	client := &Client{store: store, authorizer: AllowAllEnqueues{}}
	for _, option := range options {
		option(client)
	}
	return client
}

func (c *Client) Enqueue(ctx context.Context, batch []Envelope) error {
	return c.EnqueueWithSource(ctx, EnqueueSourceLibrary, batch)
}

// EnqueueAndWait subscribes before enqueue, then reconciles durable state after enqueue
// and periodically. This closes fast-completion, dropped-event, and reconnect races.
func (c *Client) EnqueueAndWait(ctx context.Context, envelope Envelope) (Completion, error) {
	if c.eventBus == nil {
		return Completion{}, fmt.Errorf("%w: EventBus shared with the worker is required", ErrWaitUnsupported)
	}
	inspect, ok := c.store.(InspectStore)
	if !ok {
		return Completion{}, fmt.Errorf("%w: inspectable store is required", ErrWaitUnsupported)
	}
	subscription, err := c.eventBus.Subscribe(ctx, SubscriptionConfig{})
	if err != nil {
		return Completion{}, err
	}
	defer subscription.Close()
	if err := c.Enqueue(ctx, []Envelope{envelope}); err != nil {
		return Completion{}, err
	}
	resultInspect, _ := c.store.(ResultInspectStore)
	if done, ok, err := completionFromStore(ctx, inspect, resultInspect, envelope.ID, nil); err != nil || ok {
		return done, err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return Completion{}, ctx.Err()
		case event, open := <-subscription.Events():
			if !open {
				return Completion{}, fmt.Errorf("%w: event stream closed", ErrWaitUnsupported)
			}
			if event.envelope.ID != envelope.ID {
				continue
			}
			if done, ok, err := completionFromStore(ctx, inspect, resultInspect, envelope.ID, &event); err != nil || ok {
				return done, err
			}
		case <-ticker.C:
			if done, ok, err := completionFromStore(ctx, inspect, resultInspect, envelope.ID, nil); err != nil || ok {
				return done, err
			}
		}
	}
}

func completionFromStore(
	ctx context.Context,
	inspect InspectStore,
	results ResultInspectStore,
	jobID string,
	event *JobEvent,
) (Completion, bool, error) {
	job, err := inspect.GetJob(ctx, jobID, false)
	if err != nil {
		return Completion{}, false, err
	}
	if job == nil {
		if event != nil && terminalWaitState(event.State()) {
			return Completion{JobID: jobID, State: event.State(), Error: event.ErrorMessage()}, true, nil
		}
		return Completion{}, false, nil
	}
	if !terminalWaitState(job.State) {
		return Completion{}, false, nil
	}
	var result *JobResult
	if results != nil {
		result, err = results.GetJobResult(ctx, jobID)
		if err != nil {
			return Completion{}, false, err
		}
	}
	errMessage := ""
	if event != nil {
		errMessage = event.ErrorMessage()
	}
	if errMessage == "" {
		errMessage = latestWaitError(job.ErrorsJSON)
	}
	return Completion{JobID: jobID, State: job.State, Result: result, Error: errMessage}, true, nil
}

func terminalWaitState(state string) bool {
	switch state {
	case "completed", "archived", "cancelled", "undecodable", "quarantined", "deleted":
		return true
	default:
		return false
	}
}

func latestWaitError(errorsJSON string) string {
	var entries []struct {
		Error string `json:"error"`
	}
	if json.Unmarshal([]byte(errorsJSON), &entries) != nil || len(entries) == 0 {
		return ""
	}
	return entries[len(entries)-1].Error
}

// EnqueueWithSource lets trusted adapters (notably the control API) preserve the source
// supplied to authorization while using the identical circuit and store call.
func (c *Client) EnqueueWithSource(
	ctx context.Context,
	source EnqueueSource,
	batch []Envelope,
) error {
	request := EnqueueRequest{
		Source: source, Operation: EnqueueOperationDirect, Batch: cloneEnqueueBatch(batch),
	}
	terminal := func(ctx context.Context, request EnqueueRequest) error {
		if err := AuthorizeEnqueueBatch(ctx, c.authorizer, request.Source, request.Batch); err != nil {
			return err
		}
		return c.callStore(ctx, request, func() error { return c.store.Enqueue(ctx, request.Batch) })
	}
	return newEnqueueNext(c.middlewareChain(), terminal).Run(ctx, request)
}

// EnqueueTx runs the identical authorization pass before touching the caller's
// transaction, so the transactional path is not a policy bypass.
func (c *Client) EnqueueTx(ctx context.Context, tx Tx, batch []Envelope) error {
	request := EnqueueRequest{
		Source: EnqueueSourceLibrary, Operation: EnqueueOperationTransactional,
		Batch: cloneEnqueueBatch(batch),
	}
	terminal := func(ctx context.Context, request EnqueueRequest) error {
		if err := AuthorizeEnqueueBatch(ctx, c.authorizer, request.Source, request.Batch); err != nil {
			return err
		}
		transactional, ok := c.store.(TransactionalStore)
		if !ok {
			return Invalidf("transactional enqueue is unsupported by this store")
		}
		return c.callStore(ctx, request, func() error {
			return transactional.EnqueueTx(ctx, tx, request.Batch)
		})
	}
	return newEnqueueNext(c.middlewareChain(), terminal).Run(ctx, request)
}

func (c *Client) callStore(
	ctx context.Context,
	request EnqueueRequest,
	call func() error,
) (err error) {
	var permit *circuitPermit
	if c.breaker != nil {
		permit, err = c.breaker.acquire()
		if err != nil {
			return err
		}
	}
	// If call panics, the deferred exclusion releases a half-open slot and deliberately
	// does not blame the store. The panic itself continues to the caller.
	defer func() {
		if permit != nil && !permit.completed {
			permit.exclude()
		}
	}()
	attempt := newInsertAttempt(request)
	c.emitInsert(ctx, InsertHookEvent{phase: InsertHookBegin, attempt: attempt})
	err = call()
	// Direct caller cancellation is not a store health sample. Drivers that have
	// positively classified a transport timeout wrap it as UnavailableError first.
	if permit != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			permit.exclude()
		} else {
			permit.finish(IsUnavailable(err))
		}
	}
	outcome := classifyInsertOutcome(err)
	c.emitInsert(ctx, InsertHookEvent{
		phase: InsertHookEnd, attempt: attempt, outcome: &outcome,
	})
	return err
}

func (c *Client) emitInsert(ctx context.Context, event InsertHookEvent) {
	for _, hook := range c.insertHooks {
		hook.OnInsert(ctx, event)
	}
	for _, hook := range c.globalPluginHooks {
		hook.OnInsert(ctx, event)
	}
	for _, hook := range c.scopedPluginHooks {
		hook.OnInsert(ctx, event)
	}
}
