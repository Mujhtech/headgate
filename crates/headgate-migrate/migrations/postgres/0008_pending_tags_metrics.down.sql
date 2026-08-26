-- The enum label remains until migration 1 drops the type. PostgreSQL cannot remove one
-- label in place; leaving it unused is safer than rebuilding a hot table.
SELECT 1;

