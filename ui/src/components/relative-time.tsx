import { useRelativeNow } from "@/lib/clock";
import { formatDate, formatRelativeTime } from "@/lib/format";

interface RelativeTimeProps {
  className?: string;
  value: number | null | undefined;
}

export function RelativeTime({ className, value }: RelativeTimeProps) {
  const now = useRelativeNow();
  if (!value) {
    return <span className={className}>—</span>;
  }

  return (
    <time
      className={className}
      dateTime={new Date(value).toISOString()}
      suppressHydrationWarning
      title={formatDate(value)}
    >
      {formatRelativeTime(value, now)}
    </time>
  );
}
