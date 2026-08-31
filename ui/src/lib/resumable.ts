export interface JobCheckpoint {
  last_completed_step: string | null
  completed_steps: string[]
  in_progress_step: string | null
  cursor_step: string | null
  cursor: string | null
  schema_version: number
  step_set_hash: string
  crashes_by_step: Record<string, number>
}

export function hasResumableCheckpoint(checkpoint: JobCheckpoint): boolean {
  return checkpoint.completed_steps.length > 0
    || checkpoint.in_progress_step != null
    || checkpoint.cursor_step != null
    || checkpoint.cursor != null
    || checkpoint.schema_version > 0
    || checkpoint.step_set_hash.length > 0
    || Object.keys(checkpoint.crashes_by_step).length > 0
}
