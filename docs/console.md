# Embedded operations console

The console is a TanStack Start single-page application built with React, Tailwind, and
shadcn components backed by Base UI. `ui/dist` is the canonical static artifact. The UI
build copies the same directory into `go/headgateui/dist`; verification fails if any
byte differs.

Views use TanStack file routing under `ui/src/routes`. The shared `_console` layout owns
the responsive sidebar, refresh connection, notices, and read-only status; each operator
screen owns its URL and search state. Current routes include `/queues`, `/jobs`,
`/jobs/{id}`, `/workflows`, `/workflows/{workflow-id}`, `/rate-classes`,
`/quarantine`, `/periodic`, and `/workers`.

TanStack Query owns control-API reads, polling, cancellation, mutation state, and cache
invalidation. React effects are reserved for browser subscriptions such as server-sent
events and responsive media queries. Form controls use the shadcn components backed by
Base UI rather than unstyled native checkboxes or selects.

The workflow screens use the existing job API: the list filters for
`headgate:workflow` coordinators, while the detail screen explicitly requests the
coordinator payload and performs bounded point reads for its children. It renders the
static DAG and live task states without introducing a backend-specific graph endpoint.

Neither Rust nor Go runs a JavaScript server. Each SDK embeds the HTML shell, hashed
JavaScript and CSS, and fonts into its binary. Application routes fall back to the shell;
missing `/assets/...` requests return 404 instead of HTML.

## Try every view locally

Run the real embedded console against the example's realistic, read-only API:

```bash
cd examples/go
GOWORK=off go run ./ui_demo
```

Open `http://127.0.0.1:8080`. The example includes active and blocked jobs, queue
history, partition fairness, rate classes, quarantine entries, periodic schedules,
worker coverage, progress, admission explanations, and a connected workflow DAG. It
needs no database or JavaScript development server. The fixture API is read-only and
exists only to demonstrate the operator experience; use the mounts below with a real
control API in an application.

## Build and verify

```bash
cd ui
pnpm install
pnpm check
```

`pnpm check` type-checks, tests, builds the SPA, normalizes the shell to
`ui/dist/index.html`, and refreshes the Go mirror. The root `scripts/verify.sh` runs the
same gate and compares the complete artifact trees.

For development:

```bash
cd ui
pnpm dev
```

The development server expects the control API at `/api/v1`. Use a same-origin reverse
proxy when developing against another process so cookies and browser security behavior
match production.

## Rust mount

```rust
let app = axum::Router::new()
    .nest("/api/v1", headgate_api::router(store, api_config))
    .nest_service(
        "/admin/jobs",
        headgate_ui::router(headgate_ui::Config {
            api_base: "/api/v1".into(),
            read_only: false,
        }),
    );
```

## Go mount

```go
mux.Handle("/admin/jobs/", http.StripPrefix(
    "/admin/jobs",
    headgateui.NewHandler(headgateui.Config{
        APIBase: "/api/v1",
        ReadOnly: false,
    }),
))
```

Relative asset URLs allow either handler to live below an arbitrary mount path.

## Security boundary

The console intentionally provides no authentication. Mount it behind the host
application's admin authentication and authorization. Its `read_only` option only
disables browser controls; configure the control API's read-only mode as well so direct
HTTP requests receive `403 Forbidden`.

Job payloads remain excluded unless an API caller explicitly requests them. Do not put
the console on a public route merely because the default views omit payloads: job kinds,
queues, tenant partitions, errors, logs, schedules, and worker hostnames are still
operationally sensitive.
