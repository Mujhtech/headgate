import { Progress as ProgressPrimitive } from "@base-ui/react/progress";

import { cn } from "@/lib/utils";

export function Progress({
  className,
  value,
  ...props
}: ProgressPrimitive.Root.Props) {
  return (
    <ProgressPrimitive.Root
      className={cn("w-full", className)}
      value={value}
      {...props}
    >
      <ProgressPrimitive.Track className="h-1.5 overflow-hidden rounded-full bg-muted">
        <ProgressPrimitive.Indicator className="h-full bg-primary transition-[width] motion-reduce:transition-none" />
      </ProgressPrimitive.Track>
    </ProgressPrimitive.Root>
  );
}
