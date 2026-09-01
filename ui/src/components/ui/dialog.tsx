import { Dialog as DialogPrimitive } from "@base-ui/react/dialog";
import { XIcon } from "lucide-react";
import type { ComponentProps } from "react";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export const Dialog = DialogPrimitive.Root;
export const DialogTrigger = DialogPrimitive.Trigger;
export const DialogClose = DialogPrimitive.Close;

export function DialogContent({
  className,
  children,
  ...props
}: DialogPrimitive.Popup.Props) {
  return (
    <DialogPrimitive.Portal>
      <DialogPrimitive.Backdrop className="data-open:fade-in-0 data-closed:fade-out-0 fixed inset-0 z-40 bg-black/30 backdrop-blur-[1px] data-closed:animate-out data-open:animate-in motion-reduce:animate-none" />
      <DialogPrimitive.Popup
        className={cn(
          "data-open:slide-in-from-right data-closed:slide-out-to-right fixed inset-y-0 right-0 z-50 w-full max-w-xl overflow-y-auto border-l bg-background p-5 shadow-xl outline-none data-closed:animate-out data-open:animate-in motion-reduce:animate-none",
          className
        )}
        {...props}
      >
        {children}
        <DialogPrimitive.Close
          render={
            <Button
              className="absolute top-3 right-3"
              size="icon"
              variant="ghost"
            />
          }
        >
          <XIcon />
          <span className="sr-only">Close</span>
        </DialogPrimitive.Close>
      </DialogPrimitive.Popup>
    </DialogPrimitive.Portal>
  );
}

export function DialogHeader({
  className,
  ...props
}: ComponentProps<"header">) {
  return <header className={cn("mb-5 pr-10", className)} {...props} />;
}

export function DialogTitle({
  className,
  ...props
}: DialogPrimitive.Title.Props) {
  return (
    <DialogPrimitive.Title
      className={cn("font-mono font-semibold text-sm", className)}
      {...props}
    />
  );
}

export function DialogDescription({
  className,
  ...props
}: DialogPrimitive.Description.Props) {
  return (
    <DialogPrimitive.Description
      className={cn("mt-1 text-muted-foreground text-sm", className)}
      {...props}
    />
  );
}
