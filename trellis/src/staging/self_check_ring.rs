//! The ring rows a 1-1, group-basis or `JOIN` target hasn't applied yet, as
//! `self_check` subtracts them (#1140, ADR-0013 "Quiescence").
//!
//! Capture writes a change's ring row in the change's own transaction, so
//! under one snapshot a source's current rows are exactly what its target
//! has applied plus the changes its ring still holds unapplied. A page of
//! [`super::self_check_groups`] or [`super::self_check_joins`] reads the
//! source as *weighted rows* ([`Ring::weighted_sql`]): every current row at
//! weight `+1`, and each unapplied change's images at the opposite of their
//! sign (its old image `+1`, its new image `-1`). Their aggregates are what
//! the target has applied, in the same statement that reads the target, so
//! a correct target compares equal whatever the ring holds.
//!
//! A 1-1 page ([`super::self_check`]) reads no weighted rows: a row's
//! target holds the row as of the *old image of its key's earliest unapplied
//! change* ([`Ring::one_to_one_ctes`]), or no row for an insert, and the live
//! row when the key has none. A change its page would refuse is not counted:
//! at or below the truncate floor, at or below its key's ledger entry's
//! `applied_lsn`, or seen by that entry's `basis`.
//!
//! "Unapplied" is a ring row of the source that
//!
//! - no committed drain has applied ([`super::page::ring_row_pending_sql`],
//!   the drain's own predicate), or
//! - a page parked for a key the audited definition holds (its `poison_held`
//!   row's `origin_lsn` and later: once a key is held, every change to it
//!   parks until its release, which recomputes its groups).
//!
//! Of those, an image is subtracted only where its page would apply it:
//! above the truncate floor of the hidden table it applies to, and not seen
//! by the basis of the group or cell it lands in (a basis's absolute write
//! already counted it, and the page will refuse it).
//!
//! A ring slot whose rows are all applied is not read ([`ring_slot_applied_sql`],
//! one condition per slot, evaluated once per statement), unless the
//! definition holds keys of the source: their parked rows count wherever they
//! sit. A page then reads the slots with undrained segments, not every row
//! the ring still holds.
//!
//! Some unapplied rows can't be subtracted, and say only where the target may
//! still differ ([`Ring::uncertain_images_sql`]): an image-less change (a
//! `Recompute`, or a change missing an image its op needs) and the changes of
//! a held key whose parked origin is unknown. Their groups fall back to the
//! re-check. Any other unapplied op of the source (a `TRUNCATE`, a
//! partition's truncate, a schema change) touches every group, and the whole
//! page falls back ([`Ring::global_sql`]).
//!
//! Every image is evaluated by `jsonb_populate_record` over the source's
//! row type and then by the audit's own renderer, never by the apply path's
//! staging SQL (ADR-0013's independence).

use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::ledger as ledger_schema;
use crate::pool::{quote_ident, quote_literal};

use super::converge::per_ring_table;
use super::page::{ring_row_pending_sql, ring_slot_applied_sql};

/// The weight column of [`Ring::weighted_sql`]'s rows.
pub(super) const WEIGHT: &str = "__trellis_w";

/// One source table's unapplied ring rows, for one audited definition.
pub(super) struct Ring<'a> {
    /// The source as the ring names it (`src_table`).
    pub(super) src_table: &'a str,
    /// The source, quoted and qualified: the row type its images populate.
    pub(super) source_row: String,
    /// The source's row identity, which a ring row's `key` encodes.
    pub(super) source_pk: &'a [PrimaryKeyColumn],
    /// The audited definition.
    pub(super) transform_id: i64,
    /// A prefix naming this source's CTEs, unique within the statement.
    pub(super) tag: &'a str,
}

/// Where a side of an unapplied change lands, for the page that would apply
/// it: the `ledger_truncate_floor` key above which it applies, and the
/// hidden table (quoted, qualified) and its key columns whose row's
/// `__basis` refuses it, if any.
pub(super) struct Lands<'a> {
    pub(super) floor_key: &'a str,
    pub(super) basis: Option<(&'a str, &'a [String])>,
    /// More conditions a side must meet to apply, over its populated image
    /// `r` (` and …`, or empty): a joined row with a `NULL` join-key part
    /// applies to nothing.
    pub(super) filter: String,
    /// Whether the basis is probed in [`Ring::weighted_sql`], after the
    /// page's own conditions on the weighted rows have kept only the images
    /// in its key range, rather than once per unapplied image in the CTEs. A
    /// page whose key range bounds its weighted rows wants it: it probes a
    /// page's images, not every page's. One that reads every weighted row,
    /// several times over (`JOIN`'s target part), doesn't.
    pub(super) late_probe: bool,
}

impl Ring<'_> {
    /// The name of this source's CTE `what`.
    fn cte(&self, what: &str) -> String {
        format!("__{}_{what}", self.tag)
    }

    /// The CTEs this source contributes to a page's statement, each ending
    /// with `, ` so a caller writes them before its own: the unapplied rows,
    /// their signed images, and those images kept by `lands`. Names them by
    /// [`Self::tag`].
    pub(super) fn ctes(&self, lands: &Lands<'_>) -> String {
        format!(
            "{rows} as materialized ({rows_sql}), \
             {sides} as materialized ({sides_sql}), \
             {kept} as materialized ({kept_sql}), ",
            rows = self.cte("rows"),
            rows_sql = self.rows_sql(),
            sides = self.cte("sides"),
            sides_sql = self.sides_sql(),
            kept = self.cte("kept"),
            kept_sql = self.kept_sql(lands),
        )
    }

    /// Every unapplied ring row of the source (the module doc), as
    /// `(__txid, __lsn, __key, __op, __o, __n, __x, __kind)`. `__kind` is
    /// `side` for a change whose images apply as deltas, `uncertain` for one
    /// that can't be subtracted but names its groups, and `global` for an op
    /// that touches every group.
    fn rows_sql(&self) -> String {
        let src = quote_literal(self.src_table);
        let id = self.transform_id;
        let held_keys = format!(
            "exists (select 1 from poison_held hk \
                     where hk.transform_id = {id} and hk.src_table = {src})"
        );
        per_ring_table(" union all ", |slot, table| {
            format!(
                "select r.row_txid as __txid, r.lsn as __lsn, r.key as __key, r.op as __op, \
                        r.old_image as __o, r.new_image as __n, r.transition_images as __x, \
                        case when r.op not in ('insert', 'update', 'delete', 'recompute') \
                                  then 'global' \
                             when r.op = 'recompute' or h.unknown then 'uncertain' \
                             when jsonb_typeof(r.transition_images) = 'object' then 'side' \
                             when (r.op = 'insert' or jsonb_typeof(r.old_image) = 'object') \
                              and (r.op = 'delete' or jsonb_typeof(r.new_image) = 'object') \
                                  then 'side' \
                             else 'uncertain' end as __kind \
                 from {table} r \
                 left join lateral ( \
                     select ph.origin_lsn is null as unknown, \
                            ph.origin_lsn is not null and r.origin_lsn >= ph.origin_lsn as parked \
                     from poison_held ph \
                     where ph.transform_id = {id} and ph.src_table = r.src_table \
                       and ph.key = r.key) h on true \
                 where r.src_table = {src} \
                   and (not {applied} or {held_keys}) \
                   and (coalesce(h.unknown or h.parked, false) or {pending})",
                applied = ring_slot_applied_sql(slot),
                pending = ring_row_pending_sql(slot, None),
            )
        })
    }

    /// The CTEs a 1-1 page needs of this source (ending `, `): the unapplied
    /// rows, and `{tag}_pend`, `(__k, __i)`: for each key whose unapplied
    /// changes can all be subtracted, the *old image* of its earliest
    /// unapplied change (JSON `null` for an insert). A 1-1 target holds the
    /// row as of that image, since it has applied every change before it and
    /// none from it on. A key with an unapplied row that can't be
    /// subtracted ([`Self::uncertain_keys_sql`]) isn't in it, and is compared
    /// against its live source row.
    ///
    /// A change the page that applies it would refuse (ADR-0002's I2, against
    /// the key's entry on `target`'s ledger, `target` being the target's
    /// qualified identity) never reaches the target, so it is no unapplied
    /// change here: one at or below the entry's `applied_lsn` (a later batch
    /// of the key drained first), one its `basis` saw (a Re-derive read the
    /// live row after it), and one at or below the target's truncate floor.
    /// The changes I2 refuses are always a prefix of a key's in `lsn` order,
    /// so the earliest one it doesn't is the first the target hasn't applied.
    pub(super) fn one_to_one_ctes(&self, target: &str) -> String {
        let rows = self.cte("rows");
        let pend = self.cte("pend");
        let q = |c: &str| quote_ident(c);
        format!(
            "{rows} as materialized ({rows_sql}), \
             {pend} as materialized ( \
                 select distinct on (d.__key) d.__key as __k, \
                        case when jsonb_typeof(d.__x) = 'object' then d.__x -> 'old' \
                             else d.__o end as __i \
                 from {rows} d \
                 left join {ledger} l on l.{key_col} = d.__key \
                 where d.__kind = 'side' \
                   and d.__key not in (select __key from {rows} where __kind <> 'side') \
                   and d.__key not in (select ph.key from poison_held ph \
                                       where ph.transform_id = {id} and ph.src_table = {src} \
                                         and ph.origin_lsn is null) \
                   and (l.{basis} is null or not pg_visible_in_snapshot(d.__txid, l.{basis})) \
                   and (l.{applied} is null or d.__lsn > l.{applied}) \
                   and d.__lsn > coalesce( \
                       (select floor from ledger_truncate_floor where target_table = {floor}), \
                       '0/0'::pg_lsn) \
                 order by d.__key, d.__lsn), ",
            rows_sql = self.rows_sql(),
            id = self.transform_id,
            src = quote_literal(self.src_table),
            ledger = super::one_to_one_ledger::ledger_ident(target),
            key_col = q(ledger_schema::KEY_COLUMN),
            basis = q(ledger_schema::BASIS_COLUMN),
            applied = q(ledger_schema::APPLIED_LSN_COLUMN),
            floor = quote_literal(target),
        )
    }

    /// The name of the CTE [`Self::one_to_one_ctes`] defines of each key's
    /// earliest old image.
    pub(super) fn pend_cte(&self) -> String {
        self.cte("pend")
    }

    /// The keys (`(__k)`) of a 1-1 page the comparison can't vouch for: those
    /// with an unapplied row that isn't subtractable, and held keys whose
    /// parked origin is unknown.
    pub(super) fn uncertain_keys_sql(&self) -> String {
        format!(
            "select __key as __k from {rows} where __kind = 'uncertain' \
             union select ph.key from poison_held ph \
             where ph.transform_id = {id} and ph.src_table = {src} and ph.origin_lsn is null",
            rows = self.cte("rows"),
            id = self.transform_id,
            src = quote_literal(self.src_table),
        )
    }

    /// The signed images of the `side` rows: `(__txid, __lsn, __sgn, __i)`,
    /// a change's old image at `-1` and its new image at `+1` (or its
    /// transition images), as its page stages them.
    fn sides_sql(&self) -> String {
        let rows = self.cte("rows");
        format!(
            "select __txid, __lsn, __sgn, __i from ( \
                 select __txid, __lsn, -1 as __sgn, \
                        case when jsonb_typeof(__x) = 'object' then __x -> 'old' else __o end as __i \
                 from {rows} where __kind = 'side' \
                 union all \
                 select __txid, __lsn, 1, \
                        case when jsonb_typeof(__x) = 'object' then __x -> 'new' else __n end \
                 from {rows} where __kind = 'side') s \
             where jsonb_typeof(__i) = 'object'"
        )
    }

    /// The signed images above the truncate floor (`lands`): `(__txid,
    /// __sgn, __i)`. Unless [`Lands::late_probe`], also only those the page
    /// that applies them would apply: [`Lands::filter`] and the basis.
    fn kept_sql(&self, lands: &Lands<'_>) -> String {
        let floor = format!(
            "d.__lsn > coalesce( \
                 (select floor from ledger_truncate_floor where target_table = {}), \
                 '0/0'::pg_lsn)",
            quote_literal(lands.floor_key),
        );
        let sides = self.cte("sides");
        if lands.late_probe {
            return format!("select d.__txid, d.__sgn, d.__i from {sides} d where {floor}");
        }
        format!(
            "select d.__txid, d.__sgn, d.__i from {sides} d \
             cross join lateral jsonb_populate_record(null::{src}, d.__i) as r \
             where {floor}{basis}{filter}",
            src = self.source_row,
            basis = unseen_by_basis(lands, "d", "r"),
            filter = lands.filter,
        )
    }

    /// The source's rows as the target has applied them: every current row
    /// at weight `+1` ([`WEIGHT`]) and each kept image at the opposite of its
    /// sign, as a derived table's body, its columns the source's plus the
    /// weight first. `lands` is what [`Self::ctes`] was given: under
    /// [`Lands::late_probe`], its filter and basis apply here, after any
    /// condition the page puts on these rows.
    pub(super) fn weighted_sql(&self, lands: &Lands<'_>) -> String {
        let late = if lands.late_probe {
            format!("{}{}", unseen_by_basis(lands, "k", "r"), lands.filter)
        } else {
            String::new()
        };
        format!(
            "(select 1 as {WEIGHT}, s.* from {src} s \
              union all \
              select -k.__sgn, r.* from {kept} k \
              cross join lateral jsonb_populate_record(null::{src}, k.__i) as r \
              where true{late})",
            src = self.source_row,
            kept = self.cte("kept"),
        )
    }

    /// The images naming every group an unapplied row that can't be
    /// subtracted may touch (`(__i)`, each an object over the source's
    /// columns): its own images, the current row of its key, and the parked
    /// image and groups of a held key whose parked origin is unknown. Empty
    /// when there is no such row, without reading the source.
    pub(super) fn uncertain_images_sql(&self) -> String {
        let rows = self.cte("rows");
        let src = quote_literal(self.src_table);
        let id = self.transform_id;
        let held = format!(
            "poison_held ph \
             left join lateral unnest(ph.agg_groups) as g on true \
             where ph.transform_id = {id} and ph.src_table = {src} and ph.origin_lsn is null"
        );
        format!(
            "select i as __i from {rows} \
             cross join lateral (values (__o), (__n), (__x -> 'old'), (__x -> 'new')) v(i) \
             where __kind = 'uncertain' and jsonb_typeof(i) = 'object' \
             union all \
             select ph.old_image from {held} and jsonb_typeof(ph.old_image) = 'object' \
             union all \
             select g::jsonb from {held} and g is not null \
             union all \
             select to_jsonb(s) from {source} s \
             where (exists (select 1 from {rows} where __kind = 'uncertain') \
                    or exists (select 1 from {held})) \
               and {key} in (select __key from {rows} where __kind = 'uncertain' \
                             union all select ph.key from {held})",
            source = self.source_row,
            key = ddl::pk_key_sql_expr(self.source_pk, Some("s")),
        )
    }

    /// True when the ring holds an unapplied op of the source that touches
    /// every group (the module doc).
    pub(super) fn global_sql(&self) -> String {
        format!(
            "exists (select 1 from {} where __kind = 'global')",
            self.cte("rows")
        )
    }

    /// True when any unapplied row can't be subtracted: [`Self::global_sql`],
    /// or any row of [`Self::uncertain_images_sql`]'s.
    pub(super) fn any_uncertain_sql(&self) -> String {
        format!(
            "exists (select 1 from {rows} where __kind <> 'side') \
             or exists (select 1 from poison_held ph where ph.transform_id = {id} \
                        and ph.src_table = {src} and ph.origin_lsn is null)",
            rows = self.cte("rows"),
            id = self.transform_id,
            src = quote_literal(self.src_table),
        )
    }
}

/// ` and …`: the change of the image `image` (a row of a CTE carrying its
/// `__txid`), populated as `alias`, is unseen by the `__basis` of the row
/// it lands in (`lands`), or empty when `lands` has no basis.
fn unseen_by_basis(lands: &Lands<'_>, image: &str, alias: &str) -> String {
    match lands.basis {
        Some((hidden, columns)) => format!(
            " and not coalesce(pg_visible_in_snapshot({image}.__txid, {}), false)",
            basis_probe(hidden, columns, alias)
        ),
        None => String::new(),
    }
}

/// The `__basis` of the row of `hidden` (quoted, qualified) whose key
/// columns `columns` equal `alias`'s, `NULL` matching `NULL`, or `NULL` when
/// there is none. Plain equality first, which the hidden table's key index
/// serves; only a key with a `NULL` part takes the second lookup, which no
/// index serves.
fn basis_probe(hidden: &str, columns: &[String], alias: &str) -> String {
    if columns.is_empty() {
        return format!("(select x.__basis from {hidden} x limit 1)");
    }
    let equal: Vec<String> = columns
        .iter()
        .map(|c| {
            let c = quote_ident(c);
            format!("x.{c} = {alias}.{c}")
        })
        .collect();
    let has_null: Vec<String> = columns
        .iter()
        .map(|c| format!("{alias}.{} is null", quote_ident(c)))
        .collect();
    let same: Vec<String> = columns
        .iter()
        .map(|c| {
            let c = quote_ident(c);
            format!("x.{c} is not distinct from {alias}.{c}")
        })
        .collect();
    format!(
        "coalesce((select x.__basis from {hidden} x where {}), \
                  (select x.__basis from {hidden} x where ({}) and {}))",
        equal.join(" and "),
        has_null.join(" or "),
        same.join(" and "),
    )
}
