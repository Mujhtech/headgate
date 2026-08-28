import { LoaderCircleIcon } from "lucide-react"
import type { ComponentProps, ReactNode } from "react"

import { Button } from "@/components/ui/button"

type ActionButtonProps = ComponentProps<typeof Button> & {
  pending?: boolean
  pendingLabel?: ReactNode
}

function ActionButton({ children, disabled, pending = false, pendingLabel = "Working…", ...props }: ActionButtonProps) {
  return (
    <Button disabled={disabled || pending} aria-busy={pending || undefined} {...props}>
      {pending ? <><LoaderCircleIcon className="animate-spin motion-reduce:animate-none" />{pendingLabel}</> : children}
    </Button>
  )
}

export { ActionButton }
