const number = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const dateTime = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "medium",
});
const relativeTime = new Intl.RelativeTimeFormat(undefined, {
  numeric: "auto",
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

export function formatRelativeTime(
  ms: number | null | undefined,
  now = Date.now()
) {
  if (!ms) {
    return "—";
  }

  const seconds = (ms - now) / 1000;
  const absoluteSeconds = Math.abs(seconds);
  if (absoluteSeconds < 60) {
    return relativeTime.format(Math.round(seconds), "second");
  }
  if (absoluteSeconds < 3600) {
    return relativeTime.format(Math.round(seconds / 60), "minute");
  }
  if (absoluteSeconds < 86_400) {
    return relativeTime.format(Math.round(seconds / 3600), "hour");
  }
  if (absoluteSeconds < 604_800) {
    return relativeTime.format(Math.round(seconds / 86_400), "day");
  }
  if (absoluteSeconds < 2_629_746) {
    return relativeTime.format(Math.round(seconds / 604_800), "week");
  }
  if (absoluteSeconds < 31_556_952) {
    return relativeTime.format(Math.round(seconds / 2_629_746), "month");
  }
  return relativeTime.format(Math.round(seconds / 31_556_952), "year");
}

export function formatPercent(value: number | null | undefined) {
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: 0,
    style: "percent",
  }).format(value ?? 0);
}
