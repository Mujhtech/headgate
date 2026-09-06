import { createRootRoute, HeadContent, Scripts } from "@tanstack/react-router";
import type { ReactNode } from "react";
import css from "../styles.css?url";

const siteUrl = "https://headgate.mujhtech.chatgpt.site";
const title = "Headgate — Background jobs. Fleet-wide control.";
const description =
  "Open-source background jobs for Go and Rust. Shared rate limits, tenant fairness, workflows, and resumable execution on PostgreSQL, MySQL, or Redis.";
const socialImage = `${siteUrl}/og-image.png`;
const socialImageAlt =
  "Headgate — Background jobs. Fleet-wide control. Go + Rust. Your database. One shared gate.";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { title },
      { name: "description", content: description },
      { property: "og:type", content: "website" },
      { property: "og:site_name", content: "Headgate" },
      { property: "og:locale", content: "en_US" },
      { property: "og:url", content: `${siteUrl}/` },
      { property: "og:title", content: title },
      { property: "og:description", content: description },
      { property: "og:image", content: socialImage },
      { property: "og:image:type", content: "image/png" },
      { property: "og:image:width", content: "1730" },
      { property: "og:image:height", content: "909" },
      { property: "og:image:alt", content: socialImageAlt },
      { name: "twitter:card", content: "summary_large_image" },
      { name: "twitter:title", content: title },
      { name: "twitter:description", content: description },
      { name: "twitter:image", content: socialImage },
      { name: "twitter:image:alt", content: socialImageAlt },
      { name: "theme-color", content: "#fafbf9" },
    ],
    links: [
      { rel: "canonical", href: `${siteUrl}/` },
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
