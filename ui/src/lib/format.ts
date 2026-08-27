const number = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 })
const dateTime = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "medium",
})

export function formatDuration(ms: number | null | undefined) {
  if (ms == null) return "—"
  if (ms < 1_000) return `${number.format(ms)} ms`
  if (ms < 60_000) return `${number.format(ms / 1_000)} s`
  if (ms < 3_600_000) return `${number.format(ms / 60_000)} min`
  return `${number.format(ms / 3_600_000)} hr`
}

export function formatDate(ms: number | null | undefined) {
  return ms ? dateTime.format(new Date(ms)) : "—"
}

export function formatPercent(value: number | null | undefined) {
  return new Intl.NumberFormat(undefined, {
    style: "percent",
    maximumFractionDigits: 0,
  }).format(value ?? 0)
}
