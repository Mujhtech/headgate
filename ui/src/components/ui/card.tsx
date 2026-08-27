import type { ComponentProps } from "react"

import { cn } from "@/lib/utils"

export function Card({ className, ...props }: ComponentProps<"section">) {
  return (
    <section
      className={cn("rounded-xl border bg-card text-card-foreground shadow-xs", className)}
      {...props}
    />
  )
}

export function CardHeader({ className, ...props }: ComponentProps<"header">) {
  return <header className={cn("flex items-start gap-3 p-4 pb-2", className)} {...props} />
}

export function CardTitle({ className, ...props }: ComponentProps<"h2">) {
  return <h2 className={cn("text-sm font-semibold", className)} {...props} />
}

export function CardDescription({ className, ...props }: ComponentProps<"p">) {
  return <p className={cn("text-sm text-muted-foreground", className)} {...props} />
}

export function CardContent({ className, ...props }: ComponentProps<"div">) {
  return <div className={cn("p-4 pt-2", className)} {...props} />
}

