import { QueryClientProvider } from "@tanstack/react-query";
import {
  createRootRoute,
  HeadContent,
  Link,
  Scripts,
} from "@tanstack/react-router";

import { TooltipProvider } from "@/components/ui/tooltip";
import { configBootstrapScript } from "@/lib/config-bootstrap";
import { queryClient } from "@/lib/query";
import { themeBootstrapScript } from "@/lib/theme";
import appCss from "../styles.css?url";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { name: "google", content: "notranslate" },
      { name: "theme-color", content: "#111827" },
      { title: "headgate console" },
    ],
    links: [
      { rel: "stylesheet", href: appCss },
      {
        rel: "icon",
        // Keep the static shell and the first browser render byte-identical. The
        // embedded handlers rewrite this path for nested routes and mount prefixes.
        href: "./favicon.svg",
        type: "image/svg+xml",
      },
    ],
  }),
  shellComponent: RootDocument,
  notFoundComponent: NotFound,
});

function NotFound() {
  return (
    <main className="grid min-h-svh place-items-center p-6 text-center">
      <div>
        <p className="text-muted-foreground text-sm">404</p>
        <h1 className="mt-2 text-balance font-semibold text-2xl">
          This console page does not exist
        </h1>
        <p className="mt-2 text-muted-foreground text-sm">
          Use the operator navigation to return to a supported headgate view.
        </p>
        <Link
          className="mt-5 inline-flex rounded-lg bg-primary px-4 py-2 font-medium text-primary-foreground text-sm hover:opacity-90"
          to="/overview"
        >
          Open overview
        </Link>
      </div>
    </main>
  );
}

function RootDocument({ children }: { children: React.ReactNode }) {
  return (
    <html
      className="notranslate"
      lang="en"
      suppressHydrationWarning
      translate="no"
    >
      <head>
        <HeadContent />
        <script id="headgate-theme" suppressHydrationWarning>
          {themeBootstrapScript}
        </script>
        <script id="headgate-config">
          {configBootstrapScript(
            typeof window === "undefined" ? undefined : window.HEADGATE
          )}
        </script>
      </head>
      <body className="scrollbar-thin console-scrollbar">
        <QueryClientProvider client={queryClient}>
          <TooltipProvider>{children}</TooltipProvider>
        </QueryClientProvider>
        <Scripts />
      </body>
    </html>
  );
}
