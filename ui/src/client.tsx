import { Await, RouterProvider } from "@tanstack/react-router";
import { hydrateStart } from "@tanstack/react-start/client";
import { StrictMode, startTransition } from "react";
import { createRoot } from "react-dom/client";

import { mountPathFromModuleUrl } from "./lib/mount-path";

const browserMountPath = mountPathFromModuleUrl(import.meta.url);
const routerPromise = hydrateStart().then(async (router) => {
  router.update({ basepath: browserMountPath });
  await router.load();
  return router;
});

function MountedStartClient() {
  return (
    <Await promise={routerPromise}>
      {(router) => <RouterProvider router={router} />}
    </Await>
  );
}

startTransition(() => {
  // The embeddable Go and Rust handlers serve one root-only SPA shell as the
  // fallback for every route. A deep URL therefore has no equivalent
  // server-rendered route tree to hydrate; render the resolved client route over
  // the preload shell intentionally instead of asking React to compare them.
  createRoot(document).render(
    <StrictMode>
      <MountedStartClient />
    </StrictMode>
  );
});
