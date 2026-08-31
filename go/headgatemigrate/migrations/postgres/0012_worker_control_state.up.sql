-- Worker-reported control state makes operator actions state-aware. The command remains
-- the server-to-worker mailbox; status and duties_active are acknowledged levels written
-- by the worker heartbeat.
ALTER TABLE headgate_worker
  ADD COLUMN status text NOT NULL DEFAULT 'running',
  ADD COLUMN duties_active boolean NOT NULL DEFAULT true;
