const number = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const dateTime = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "medium",
});

export function formatDuration(ms: number | null | undefined) {
  if (ms == null) {
    return "—";
  }
  if (ms < 1000) {
    return `${number.format(ms)} ms`;
  }
  if (ms < 60_000) {
    return `${number.format(ms / 1000)} s`;
  }
  if (ms < 3_600_000) {
    return `${number.format(ms / 60_000)} min`;
  }
  return `${number.format(ms / 3_600_000)} hr`;
}

export function formatDate(ms: number | null | undefined) {
  return ms ? dateTime.format(new Date(ms)) : "—";
}

export function formatPercent(value: number | null | undefined) {
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 0,
    style: "percent",
  }).format(value ?? 0);
}
