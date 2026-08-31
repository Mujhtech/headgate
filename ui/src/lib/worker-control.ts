export type WorkerCommand = "quiet" | "resume" | "restart" | "resign" | "terminate"
export type WorkerStatus = "running" | "quiet" | "restarting" | "terminating"

export interface WorkerControlState {
  status?: WorkerStatus
  duties_active?: boolean
  pending_command?: WorkerCommand | null
}

export function workerActionDisabledReason(worker: WorkerControlState, command: WorkerCommand): string | null {
  if (worker.pending_command) return `Waiting for the worker to acknowledge ${worker.pending_command}.`
  const status = worker.status ?? "running"
  if (status === "restarting" || status === "terminating") return `Worker is ${status}.`
  if (command === "quiet" && status !== "running") return "Worker is already quiet."
  if (command === "resume" && status !== "quiet") return "Resume is available only while the worker is quiet."
  if (command === "resign" && worker.duties_active === false) return "Worker has already resigned its singleton duties."
  return null
}
