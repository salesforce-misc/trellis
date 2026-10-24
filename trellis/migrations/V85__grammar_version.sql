-- Grammar version on every stored definition (#864, ADR-0018).
--
-- `grammar_version` is the version of the definition grammar the text was
-- written under, stamped whenever the text is written (define, declare,
-- ALTER TRANSFORM). Every read of stored text checks it against the binary's
-- window (`defs::grammar`). Rows that exist when the column is added were
-- written under grammar 1.
alter table transform_definitions
    add column grammar_version integer not null default 1;
