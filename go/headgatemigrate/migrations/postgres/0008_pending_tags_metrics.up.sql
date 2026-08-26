-- v8: add the forward-only pending state in its own transaction.
-- PostgreSQL forbids using a newly added enum value in an index predicate until commit,
-- so v9 updates uniqueness and adds the related tables.
ALTER TYPE headgate_state ADD VALUE IF NOT EXISTS 'pending' BEFORE 'scheduled';

