import { Await, RouterProvider } from "@tanstack/react-router";
import { hydrateStart } from "@tanstack/react-start/client";
import { StrictMode, startTransition } from "react";
import { hydrateRoot } from "react-dom/client";

import { mountPathFromModuleUrl } from "./lib/mount-path";

const routerPromise = hydrateStart().then(async (router) => {
  router.update({ basepath: mountPathFromModuleUrl(import.meta.url) });
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
  hydrateRoot(
    document,
    <StrictMode>
      <MountedStartClient />
    </StrictMode>
  );
});
