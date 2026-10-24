-- ADR-0017 P5 (#1064): a leaf or middle `TRUNCATE` queues that node's
-- correction.
--
-- 1. The ring accepts the `node_truncate` marker a partitioned source's
--    truncate function writes for a node that is not the root (key
--    `staging::append::NODE_TRUNCATE_SENTINEL_KEY`, `new_image` =
--    `{"node": "<schema>.<table>", "oid": <oid>}`), always with a
--    `new_image` and never with an `old_image`.
--
--    This migration `ALTER`s every `seg_N`, which takes `ACCESS EXCLUSIVE`
--    and so stalls every captured application writer while it runs.

alter table seg_0 drop constraint seg_0_op_check;
alter table seg_0 add constraint seg_0_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate',
                  'schema_changed', 'node_truncate'));
alter table seg_0 add constraint seg_0_node_truncate_shape
    check (op <> 'node_truncate' or (old_image is null and new_image is not null));

alter table seg_1 drop constraint seg_1_op_check;
alter table seg_1 add constraint seg_1_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate',
                  'schema_changed', 'node_truncate'));
alter table seg_1 add constraint seg_1_node_truncate_shape
    check (op <> 'node_truncate' or (old_image is null and new_image is not null));

alter table seg_2 drop constraint seg_2_op_check;
alter table seg_2 add constraint seg_2_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate',
                  'schema_changed', 'node_truncate'));
alter table seg_2 add constraint seg_2_node_truncate_shape
    check (op <> 'node_truncate' or (old_image is null and new_image is not null));

alter table seg_3 drop constraint seg_3_op_check;
alter table seg_3 add constraint seg_3_op_check
    check (op in ('insert', 'update', 'delete', 'recompute', 'truncate',
                  'schema_changed', 'node_truncate'));
alter table seg_3 add constraint seg_3_node_truncate_shape
    check (op <> 'node_truncate' or (old_image is null and new_image is not null));

-- 2. The seal queues a `truncated` correction (`cause = 'truncated'`) for
--    each node a marker in the segment's fenced window names, in the
--    statement that publishes the fence, so each marker is read once. It
--    routes, as a `new_leaf` does, by the node's recorded constraint text,
--    and reads the node's current rows.
--
--    `partition_corrections_once` doesn't cover it: a truncate after the
--    node's correction started must queue another, since the started one may
--    have walked past the rows the truncate removed. The seal adds none
--    while an unstarted `new_leaf` or `truncated` correction of the node is
--    queued.
alter table partition_corrections drop constraint partition_corrections_kind_check;
alter table partition_corrections add constraint partition_corrections_kind_check
    check (kind in ('new_leaf', 'gone_node', 'truncated'));

drop index partition_corrections_once;
create unique index partition_corrections_once on partition_corrections
    (table_name, kind, node_oid, (md5(coalesce(constraint_text, ''))))
    where kind <> 'truncated';
