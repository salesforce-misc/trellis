-- #894: how many times the staging worker's re-type of a resume request's
-- copies (`staging::quarantine::finish_requested_resume`) was cancelled by a
-- `statement_timeout` (SQLSTATE 57014) since the request was made.
--
-- Trellis honours the operator's `statement_timeout`, so a re-type whose
-- rewrite outlasts it is cancelled, rolled back and retried on the next
-- capture pass, with its table locked `ACCESS EXCLUSIVE` for up to the
-- timeout each time. The request ends once the count reaches
-- `staging::quarantine::RETYPE_TIMEOUT_CANCEL_LIMIT`. `request_retype`'s
-- upsert resets it to 0, so each `RESUME` starts fresh. Only 57014 counts: a
-- lock timeout or a lost connection never held the lock for long.
alter table resume_requests
    add column timeout_cancels integer not null default 0;
