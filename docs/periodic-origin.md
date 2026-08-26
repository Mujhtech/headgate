# Periodic job origin

Every scheduler-created job stores two typed fields: `periodic_schedule_id` and
`periodic_tick_ms`. The inspection API exposes them as `periodic_origin`; ordinary jobs
return `null`. The pair is constrained together at the validation and database boundaries.

This is intentionally separate from the generated job ID, uniqueness key, and opaque
headers. Those values may change representation without breaking operator queries or
forcing users to parse an implementation detail.
