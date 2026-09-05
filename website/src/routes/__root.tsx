import { createRootRoute, HeadContent, Scripts } from "@tanstack/react-router";
import type { ReactNode } from "react";
import css from "../styles.css?url";
export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { title: "Headgate — Background jobs. Fleet-wide control." },
      {
        name: "description",
        content:
          "Open-source background jobs for Go and Rust. Shared rate limits, tenant fairness, workflows, and resumable execution on PostgreSQL, MySQL, or Redis.",
      },
      { name: "theme-color", content: "#fafbf9" },
    ],
    links: [
      { rel: "stylesheet", href: css },
      { rel: "icon", href: "/favicon.svg", type: "image/svg+xml" },
    ],
  }),
  shellComponent: ({ children }: { children: ReactNode }) => (
    <html lang="en" suppressHydrationWarning>
      <head>
        <HeadContent />
        <script>{`(() => { let theme; try { theme = localStorage.getItem('headgate-website-theme'); } catch {} document.documentElement.dataset.theme = theme === 'light' || theme === 'dark' ? theme : matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'; })();`}</script>
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  ),
  notFoundComponent: () => (
    <main className="not-found">
      <h1>This page took a different route.</h1>
      <a href="/">Back to Headgate →</a>
    </main>
  ),
});
