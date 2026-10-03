-- #625 F8b: field-scoped Re-derive builds. `ALTER TRANSFORM ... ADD/ALTER`
-- and a column resume no longer recompute the field inside the call: they
-- move the definition `live -> backfilling` (`build = 'rederive'`) and
-- enqueue a plan job whose chunks re-derive only the named fields
-- (`staging::build`'s module doc, "Field builds").
--
-- `backfill_chunks.fields` is that scope: the target columns a `plan` row
-- and the `rederive` rows it enqueues write. `null` is a whole build, which
-- writes every field.
alter table backfill_chunks
    add column fields text[],
    add constraint backfill_chunks_fields_kind check (
        fields is null or kind in ('plan', 'rederive')
    );
