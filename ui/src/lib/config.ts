export interface HeadgateConfig {
  apiBase: string;
  readOnly: boolean;
}

declare global {
  interface Window {
    HEADGATE?: Partial<HeadgateConfig>;
  }
}

export const config: HeadgateConfig = {
  apiBase: window.HEADGATE?.apiBase ?? "/api/v1",
  readOnly: window.HEADGATE?.readOnly ?? false,
};
