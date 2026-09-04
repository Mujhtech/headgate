import { cp, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const uiRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const clientBuild = join(uiRoot, ".tanstack", "client");
const canonicalBuild = join(uiRoot, "dist");
const goBuild = join(uiRoot, "..", "go", "headgateui", "dist");

await rm(canonicalBuild, { force: true, recursive: true });
await mkdir(canonicalBuild, { recursive: true });
await cp(clientBuild, canonicalBuild, { recursive: true });
await rename(
  join(canonicalBuild, "_shell.html"),
  join(canonicalBuild, "index.html")
);

const indexPath = join(canonicalBuild, "index.html");
const index = await readFile(indexPath, "utf8");
await writeFile(
  indexPath,
  index
    .replace("<!DOCTYPE html>", "<!doctype html><!-- headgate console -->")
    .replaceAll("/./assets/", "./assets/")
);

await rm(goBuild, { force: true, recursive: true });
await cp(canonicalBuild, goBuild, { recursive: true });
