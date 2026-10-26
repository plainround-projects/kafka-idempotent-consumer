-- Back to a clean start for the next experiment: every balance zero, no processed events.
-- Not part of the numbered run order; make reset-db runs it.

UPDATE balances SET total = 0, updated_at = now();

DO $$
BEGIN
    IF to_regclass('processed_events') IS NOT NULL THEN
        TRUNCATE processed_events;
    END IF;
END
$$;
