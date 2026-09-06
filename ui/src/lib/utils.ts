import { type ClassValue, cn as cnFn } from "cn";

export function cn(...inputs: ClassValue[]) {
  return cnFn(...inputs);
}
