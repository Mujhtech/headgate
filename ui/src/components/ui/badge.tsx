import type { ComponentProps } from "react"
import { cva, type VariantProps } from "class-variance-authority"

import { cn } from "@/lib/utils"

const badgeVariants = cva(
  "inline-flex h-5 w-fit items-center rounded-full px-2 text-xs font-medium",
  {
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground",
        secondary: "bg-muted text-muted-foreground",
        outline: "border border-border text-foreground",
        destructive: "bg-destructive/10 text-destructive",
        success: "bg-success/10 text-success",
        warning: "bg-warning/10 text-warning",
      },
    },
    defaultVariants: { variant: "secondary" },
  },
)

export function Badge({
  className,
  variant,
  ...props
}: ComponentProps<"span"> & VariantProps<typeof badgeVariants>) {
  return <span className={cn(badgeVariants({ variant }), className)} {...props} />
}

