import type { HeadgateConfig } from "./config-bootstrap";

export type { HeadgateConfig } from "./config-bootstrap";

export const config: HeadgateConfig = {
  apiBase: window.HEADGATE?.apiBase ?? "/api/v1",
  readOnly: window.HEADGATE?.readOnly ?? false,
};
