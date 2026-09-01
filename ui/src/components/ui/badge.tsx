import { cva, type VariantProps } from "class-variance-authority";
import type { ComponentProps } from "react";

import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex h-5 w-fit items-center rounded-full px-2 font-medium text-xs",
  {
    defaultVariants: { variant: "secondary" },
    variants: {
      variant: {
        default: "bg-primary text-primary-foreground",
        destructive: "bg-destructive/10 text-destructive",
        outline: "border border-border text-foreground",
        secondary: "bg-muted text-muted-foreground",
        success: "bg-success/10 text-success",
        warning: "bg-warning/10 text-warning",
      },
    },
  }
);

export function Badge({
  className,
  variant,
  ...props
}: ComponentProps<"span"> & VariantProps<typeof badgeVariants>) {
  return (
    <span className={cn(badgeVariants({ variant }), className)} {...props} />
  );
}
