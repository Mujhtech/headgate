export interface HeadgateConfig {
  apiBase: string;
  readOnly: boolean;
}

declare global {
  interface Window {
    HEADGATE?: Partial<HeadgateConfig>;
  }
}

export const defaultHeadgateConfig: HeadgateConfig = {
  apiBase: "/api/v1",
  readOnly: false,
};

function htmlSafeJson(value: HeadgateConfig) {
  return JSON.stringify(value)
    .replaceAll("&", "\\u0026")
    .replaceAll("<", "\\u003c")
    .replaceAll(">", "\\u003e")
    .replaceAll("\u2028", "\\u2028")
    .replaceAll("\u2029", "\\u2029");
}

export function configBootstrapScript(
  configured: Partial<HeadgateConfig> | undefined
) {
  const config: HeadgateConfig = {
    apiBase: configured?.apiBase ?? defaultHeadgateConfig.apiBase,
    readOnly: configured?.readOnly ?? defaultHeadgateConfig.readOnly,
  };
  return `window.HEADGATE = ${htmlSafeJson(config)};`;
}
