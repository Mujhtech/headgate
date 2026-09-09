const ASSET_DIRECTORY = "/assets/";

/**
 * Derives the console's browser-visible mount point from the entry module URL.
 *
 * The Go and Rust packages can be mounted behind a prefix that they never see
 * (http.StripPrefix / axum nest_service). The entry module is still requested
 * from that prefix, so its URL is the one reliable source of the public mount.
 */
export function mountPathFromModuleUrl(moduleUrl: string): string {
  const { pathname } = new URL(moduleUrl);
  const assetIndex = pathname.lastIndexOf(ASSET_DIRECTORY);
  if (assetIndex < 0) {
    return "/";
  }

  return pathname.slice(0, assetIndex) || "/";
}

/** Resolves a public console file next to the embedded assets directory. */
export function consolePublicAssetUrl(
  filename: string,
  moduleUrl = import.meta.url
): string {
  return new URL(`../${filename}`, moduleUrl).href;
}
