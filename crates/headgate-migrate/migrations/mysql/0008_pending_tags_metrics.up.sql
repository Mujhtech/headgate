-- v8 is intentionally empty on MySQL. PostgreSQL needs a committed enum-extension
-- transaction before v9 can reference pending; aligned versions keep mixed deployments deterministic.
SELECT 1;

