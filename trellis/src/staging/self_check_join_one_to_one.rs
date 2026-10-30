//! `self_check` of a 1-1 `JOIN` target (ADR-0013 "Scope", ADR-0021).
//!
//! A 1-1 `JOIN` target is audited in six parts, walked in this order, each a
//! keyset walk of its own keys:
//!
//! 1. **The target** (`t`), a page of `a`'s primary key (the target's), against
//!    the live join: each field as the definition writes it over the `a` row
//!    and the `b` row on its key, and the target's row. A key one side has and
//!    the other lacks is a missing or extra row, a field that differs a cell.
//! 2. **`R` against `a`** (`r`), a page of `a`'s primary key: `a`'s rows and
//!    their join key and read columns, against `R`'s live entries. A row with
//!    no live entry, or an entry with other values, is a cell of `R`.
//! 3. **`R`'s entries with no `a` row** (`ro`), a page of `R`'s live entries by
//!    their key text: an entry whose `a` row is gone.
//! 4. **`M` against `b`** (`m`) and 5. **`M`'s entries with no `b` row**
//!    (`mo`), the same for the joined side.
//! 6. **Duplicates** (`d`), a page of `M`'s live entries in join-key order,
//!    each key among them counted over all of `M`: a key that holds more than
//!    one live entry. `b`'s `ON` columns are unique, and the target reads the
//!    newest live entry of a key, which masks a duplicate: this is where
//!    `b` losing its uniqueness shows, before a revalidation pauses the
//!    transform.
//!
//! A divergence of a hidden table is a [`Divergence::Cell`] whose column is
//! `join1_<id>_<r|m>.<column>`, and whose key is the entry's: the primary key
//! text of its row. An entry on one side only is a cell whose column is
//! `__entry` (`live` against nothing), so a row whose every value is `NULL` is
//! reported as well. A key with duplicates is a cell `join1_<id>_m.live_entries`
//! with the count of live entries against `1`, keyed by the join key's text.
//!
//! The walk's cursor ([`super::self_check::SelfCheckScope::after`]) carries the
//! part: `t:<key>`, `r:<key>`, `ro:<key>`, `m:<key>`, `mo:<key>` or `d:<key>`
//! continues a part after a key, and a bare tag starts one. The first page of a
//! walk (no cursor) starts the target's part, and the last part's end ends the
//! walk.
//!
//! **What each part compares against.** The target is a function of `R` and
//! `M`: every writer of a row holds its join key's `J` row and recomputes the
//! rows it affects (ADR-0021). `R` and `M` are the 1-1 ledger's copies of `a`
//! and `b`. So each part reads a source as its copy has applied it
//! ([`super::self_check_ring`]): a key with an unapplied change is its earliest
//! such change's old image, any other key its live row. The target part joins
//! `a` as `R` has applied it to `b` as `M` has, each `a` row to the `b` row
//! the newest live entry of its key names (the apply's rule: the latest applied
//! position, then the highest key), so a target that follows `R` and `M` agrees
//! while changes are in flight, in one pass.
//!
//! A divergence on a key the ring can't vouch for exactly (an unapplied row
//! with no usable image, an op that touches every key) is re-checked after a
//! fresh wait in `Standard` mode, like every comparison that reads a ring. The
//! duplicate part can't subtract: a hand-off of a join key between two `b` rows
//! holds two live entries for as long as the two changes are applied out of
//! order, so it re-checks whenever `b`'s ring holds an unapplied row.
//!
//! The SQL is rendered from the definition separately from the apply path's
//! (`super::join_one_to_one`): the fields by [`super::self_check_joins`]'s
//! renderer, which shares nothing with it, so a rendering bug in the apply
//! disagrees with the audit instead of agreeing with itself.
//!
//! Cost: `a`'s and `b`'s parts are range scans of the source's primary key
//! index, probing `R` or `M` by their primary key; a page of the target part
//! probes `b` by its join key, `M` by its primary key. A part driven by the
//! hidden table (`ro`, `mo`) reads a page of its keys, then looks each up in the
//! source by primary key, from the key's decoded columns. The duplicates part
//! reads a page of `M`'s entries by its join-key index, and counts each key's
//! entries over the same index.

use std::collections::{BTreeSet, HashMap, HashSet};

use tokio_postgres::types::PgLsn;

use crate::defs::ast::{JoinKind, TransformDef};
use crate::defs::catalog;
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::join::{self as spec, Side as JoinSide};
use crate::defs::join_one_to_one::HiddenTable;
use crate::defs::ledger as schema;
use crate::defs::model::Definition;
use crate::pool::{Pool, quote_ident};

use super::one_to_one_ledger::LedgerRef;
use super::self_check::{ComparePass, Divergence, Page, SelfCheckError, SelfCheckScope, diff_page};
use super::self_check_groups::Uncertain;
use super::self_check_joins::render;
use super::self_check_keyset::Keyset;
use super::self_check_ring::Ring;

/// One part of the walk (the module doc), in walk order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    /// The target against the live join.
    Target,
    /// `R` against `a`.
    From,
    /// `R`'s live entries against `a`.
    FromOrphans,
    /// `M` against `b`.
    Joined,
    /// `M`'s live entries against `b`.
    JoinedOrphans,
    /// Keys with more than one live `M` entry.
    Duplicates,
}

impl Part {
    const ALL: [Part; 6] = [
        Part::Target,
        Part::From,
        Part::FromOrphans,
        Part::Joined,
        Part::JoinedOrphans,
        Part::Duplicates,
    ];

    fn tag(self) -> &'static str {
        match self {
            Part::Target => "t",
            Part::From => "r",
            Part::FromOrphans => "ro",
            Part::Joined => "m",
            Part::JoinedOrphans => "mo",
            Part::Duplicates => "d",
        }
    }

    fn of_tag(tag: &str) -> Option<Part> {
        Self::ALL.into_iter().find(|part| part.tag() == tag)
    }

    fn next(self) -> Option<Part> {
        let at = Self::ALL.iter().position(|part| *part == self)?;
        Self::ALL.get(at + 1).copied()
    }
}

/// The part and the key within it a cursor continues after (the module doc).
/// A cursor of no part's form continues the target's part after it.
fn parse_cursor(after: Option<&str>) -> (Part, Option<String>) {
    let Some(after) = after else {
        return (Part::Target, None);
    };
    match after.split_once(':') {
        Some((tag, key)) => match Part::of_tag(tag) {
            Some(part) => (part, Some(key.to_string())),
            None => (Part::Target, Some(after.to_string())),
        },
        None => match Part::of_tag(after) {
            Some(part) => (part, None),
            None => (Part::Target, Some(after.to_string())),
        },
    }
}

/// The cursor that follows a page of `part` that ended at `page_end`
/// (`None`: the part's end).
fn next_cursor(part: Part, page_end: Option<String>) -> Option<String> {
    match page_end {
        Some(key) => Some(format!("{}:{key}", part.tag())),
        None => part.next().map(|next| next.tag().to_string()),
    }
}

/// The column a divergence of a hidden table's entry that exists on one side
/// only names.
const ENTRY: &str = "__entry";

/// What an entry's values start with on each side that has it, so a row whose
/// values are all `NULL` still differs from no row.
const LIVE: &str = "live";

/// Refuses a 1-1 `JOIN` definition with a field the comparison can't render.
pub(super) fn check_renders(def: &Definition) -> Result<(), SelfCheckError> {
    for field in &def.def.fields {
        render(&def.def, &field.expr, "a", "b").map_err(|detail| {
            SelfCheckError::UnsupportedExpr {
                target: def.def.target.clone(),
                detail,
            }
        })?;
    }
    Ok(())
}

/// One side of the join: its source, the hidden ledger that copies it and the
/// columns the ledger holds.
struct Side {
    /// `r` or `m`, the ledger's part of its name.
    ledger_name: String,
    /// The tag of this side's ring CTEs.
    tag: &'static str,
    /// The source, qualified and unquoted: what the ring names it.
    table: String,
    /// The source, qualified and quoted.
    ident: String,
    /// The source's primary key, which the ledger's keys encode.
    pk: Vec<PrimaryKeyColumn>,
    ledger: LedgerRef,
    /// The columns the ledger copies, in the order a page compares them.
    columns: Vec<String>,
    /// The side's join key columns, in `ON` order.
    key: Vec<String>,
}

impl Side {
    fn ring(&self, id: i64) -> Ring<'_> {
        Ring {
            src_table: &self.table,
            source_row: self.ident.clone(),
            source_pk: &self.pk,
            transform_id: id,
            tag: self.tag,
        }
    }

    /// The source's rows as its ledger has applied them: the live row of a key
    /// with no unapplied change, the old image of its earliest unapplied change
    /// otherwise (no row for an insert), as a derived table's body.
    fn applied(&self, ring: &Ring<'_>) -> String {
        let pend = ring.pend_cte();
        format!(
            "(select s.* from {ident} s \
              where not exists (select 1 from {pend} p where p.__k = {skey}) \
              union all \
              select r.* from {pend} p \
              cross join lateral jsonb_populate_record(null::{ident}, p.__i) r \
              where jsonb_typeof(p.__i) = 'object')",
            ident = self.ident,
            skey = ddl::pk_key_sql_expr(&self.pk, Some("s")),
        )
    }

    /// The name of a divergence's column in this side's ledger.
    fn divergence_column(&self, column: &str) -> String {
        format!("{}.{column}", self.ledger_name)
    }
}

/// One field of the target.
struct Field {
    name: String,
    /// The field over the aliases `a` and `b`, cast to its target column's
    /// type.
    sql: String,
}

/// What the audit reads of the definition, read once per page.
struct Audit<'a> {
    def: &'a Definition,
    left: bool,
    a: Side,
    b: Side,
    /// The fields the page compares: not the paused ones.
    fields: Vec<Field>,
}

impl Audit<'_> {
    fn id(&self) -> i64 {
        self.def.id
    }

    fn side(&self, part: Part) -> &Side {
        match part {
            Part::From | Part::FromOrphans => &self.a,
            _ => &self.b,
        }
    }

    /// The `ON` terms of the join, over the aliases `a` and `x`.
    fn on(&self) -> String {
        self.a
            .key
            .iter()
            .zip(&self.b.key)
            .map(|(a, b)| format!("a.{} = x.{}", quote_ident(a), quote_ident(b)))
            .collect::<Vec<_>>()
            .join(" and ")
    }

    /// The expected target: each `a` row as `R` has applied it, with the fields
    /// of the `b` row on its key, as `M` has applied it, the newest live entry's
    /// when a key has several (a derived table named `e`, its columns `a`'s
    /// primary key's and `__f<i>` for each field).
    fn expected_target(&self, pk: &[PrimaryKeyColumn]) -> String {
        let a_ring = self.a.ring(self.id());
        let b_ring = self.b.ring(self.id());
        let mut columns: Vec<String> = pk
            .iter()
            .map(|c| format!("a.{n} as {n}", n = quote_ident(&c.name)))
            .collect();
        columns.extend(
            self.fields
                .iter()
                .enumerate()
                .map(|(i, f)| format!("({}) as __f{i}", f.sql)),
        );
        let b_key = ddl::pk_key_sql_expr(&self.b.pk, Some("x"));
        let m = &self.b.ledger.ident;
        let join = if self.left {
            "left join lateral"
        } else {
            "join lateral"
        };
        format!(
            "(select {columns} from {a_applied} a \
              {join} (select x.* from {b_applied} x \
                      left join {m} mm on mm.{key} = ({b_key}) collate \"C\" and not mm.{tomb} \
                      where {on} \
                      order by mm.{applied} desc nulls last, {b_key} collate \"C\" desc \
                      limit 1) b on true) e",
            columns = columns.join(", "),
            a_applied = self.a.applied(&a_ring),
            b_applied = self.b.applied(&b_ring),
            key = quote_ident(schema::KEY_COLUMN),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
            applied = quote_ident(schema::APPLIED_LSN_COLUMN),
            on = self.on(),
        )
    }

    /// The statement of a page of the target part. `$1` is the page's limit.
    /// Each row is `(side, page_end, key, values)`: side 1 the expected target,
    /// 2 the persisted one, 3 a key the page can't vouch for, 4 every key.
    fn target_sql(&self, keyset: &Keyset) -> String {
        let refs = keyset.refs(None);
        let carried = keyset.select_carried(&refs);
        let order = keyset.order_by(&refs);
        let key = ddl::pk_key_sql_expr(keyset.columns(), None);
        let expected = self.expected_target(keyset.columns());
        let recomputed_values: Vec<String> = (0..self.fields.len())
            .map(|i| format!("__f{i}::text"))
            .collect();
        let persisted_values: Vec<String> = self
            .fields
            .iter()
            .map(|f| format!("{}::text", quote_ident(&f.name)))
            .collect();
        let target = ddl::qualified_target_table_ident(&self.def.target_table);
        let side = |from: &str, values: &[String]| {
            keyset.side(&refs, |cond| {
                format!(
                    "select {key} as __k{carried}, array[{}]::text[] as __v from {from} \
                     where {cond} order by {order} limit $1",
                    values.join(", ")
                )
            })
        };
        let a_ring = self.a.ring(self.id());
        let b_ring = self.b.ring(self.id());
        keyset.ranked(
            &format!(
                "{a_ctes}{b_ctes}recomputed as materialized ({recomputed}), \
                 persisted as materialized ({persisted}), ",
                a_ctes = a_ring.one_to_one_ctes(&self.a.ledger),
                b_ctes = b_ring.one_to_one_ctes(&self.b.ledger),
                recomputed = side(&expected, &recomputed_values),
                persisted = side(&target, &persisted_values),
            ),
            &[(1, "recomputed"), (2, "persisted")],
            &[1, 2],
            &format!(
                " union all select 3, page.page_end, u.__k, null::text[] \
                    from page, ({a_unc}) u \
                  union all select 4, page.page_end, '', null::text[] from page \
                   where {a_global} or {b_global} or exists ({b_unc})",
                a_unc = a_ring.uncertain_keys_sql(),
                a_global = a_ring.global_sql(),
                b_global = b_ring.global_sql(),
                b_unc = b_ring.uncertain_keys_sql(),
            ),
        )
    }

    /// The statement of a page of a ledger's part against its source, `side`'s.
    /// `$1` is the limit. Each row is `(side, page_end, key, values)`: side 1
    /// the source's rows as the ledger has applied them, 2 the ledger's live
    /// entries on those keys, 3 a key the page can't vouch for, 4 every key.
    /// The values lead with [`LIVE`], then the columns the ledger copies.
    fn ledger_sql(&self, side: &Side, keyset: &Keyset) -> String {
        let refs = keyset.refs(None);
        let carried = keyset.select_carried(&refs);
        let order = keyset.order_by(&refs);
        let key = ddl::pk_key_sql_expr(keyset.columns(), None);
        let ring = side.ring(self.id());
        let expected_values: Vec<String> = side
            .columns
            .iter()
            .map(|c| format!("{}::text", quote_ident(c)))
            .collect();
        let persisted_values: Vec<String> = side
            .columns
            .iter()
            .map(|c| format!("r.{}::text", quote_ident(c)))
            .collect();
        let expected = keyset.side(&refs, |cond| {
            format!(
                "select {key} as __k{carried}, array['{LIVE}', {}]::text[] as __v \
                 from {applied} e where {cond} order by {order} limit $1",
                expected_values.join(", "),
                applied = side.applied(&ring),
            )
        });
        let carried_e: String = (0..keyset.columns().len())
            .map(|i| format!(", e.__c{i} as __c{i}"))
            .collect();
        keyset.ranked(
            &format!(
                "{ctes}expected as materialized ({expected}), \
                 persisted as materialized ( \
                     select e.__k{carried_e}, array['{LIVE}', {persisted}]::text[] as __v \
                     from expected e \
                     join {ledger} r on r.{key_col} = e.__k collate \"C\" and not r.{tomb}), ",
                ctes = ring.one_to_one_ctes(&side.ledger),
                persisted = persisted_values.join(", "),
                ledger = side.ledger.ident,
                key_col = quote_ident(schema::KEY_COLUMN),
                tomb = quote_ident(schema::TOMBSTONE_COLUMN),
            ),
            &[(1, "expected"), (2, "persisted")],
            &[1],
            &format!(
                " union all select 3, page.page_end, u.__k, null::text[] \
                    from page, ({unc}) u \
                  union all select 4, page.page_end, '', null::text[] from page \
                   where {global}",
                unc = ring.uncertain_keys_sql(),
                global = ring.global_sql(),
            ),
        )
    }

    /// The statement that finds, among the keys `$1` (their text) and `$2` (the
    /// same keys as JSON objects of their primary key's columns), the live
    /// entries of `side`'s ledger with no row in the source as the ledger has
    /// applied it. Each row is `(kind, key)`: kind 0 an entry with no row, 3 a
    /// key the page can't vouch for, 4 every key.
    fn orphans_sql(&self, side: &Side) -> String {
        let ring = side.ring(self.id());
        let matched: Vec<String> = side
            .pk
            .iter()
            .map(|c| {
                let n = quote_ident(&c.name);
                if c.nullable {
                    format!("e.{n} is not distinct from kk.{n}")
                } else {
                    format!("e.{n} = kk.{n}")
                }
            })
            .collect();
        format!(
            "with {ctes}\
             keys as materialized ( \
                 select u.__k, k.* \
                 from unnest($1::text[], $2::text[]) as u(__k, __j) \
                 cross join lateral jsonb_populate_record(null::{ident}, u.__j::jsonb) k), \
             orphans as ( \
                 select kk.__k from keys kk \
                 where exists (select 1 from {ledger} r \
                               where r.{key_col} = kk.__k and not r.{tomb}) \
                   and not exists (select 1 from {applied} e where {matched})) \
             select 0, __k from orphans \
             union all select 3, u.__k from ({unc}) u where u.__k in (select __k from keys) \
             union all select 4, '' where {global}",
            ctes = ring.one_to_one_ctes(&side.ledger),
            ident = side.ident,
            ledger = side.ledger.ident,
            key_col = quote_ident(schema::KEY_COLUMN),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
            applied = side.applied(&ring),
            matched = matched.join(" and "),
            unc = ring.uncertain_keys_sql(),
            global = ring.global_sql(),
        )
    }
}

/// One part's page: [`ComparePass`] minus the token.
struct Pass {
    rows_compared: i64,
    /// Where the part's next page starts, or `None` when it ended.
    page_end: Option<String>,
    divergences: Vec<Divergence>,
    uncertain: Uncertain,
}

/// One keyset page of the audit of `def`, the 1-1 `JOIN` definition whose
/// target is keyed by `pk` (`a`'s primary key), from the part and key `scope`
/// names (the module doc).
pub(super) async fn compare(
    pool: &Pool,
    def: &Definition,
    pk: &[PrimaryKeyColumn],
    scope: &SelfCheckScope,
    paused: &HashSet<String>,
    checked_through: PgLsn,
) -> Result<ComparePass, SelfCheckError> {
    let (part, after) = parse_cursor(scope.after.as_deref());
    let audit = build(pool, def, pk, paused).await?;
    let pass = match part {
        Part::Target => target_page(pool, &audit, pk, after.as_deref(), scope.limit).await?,
        Part::From | Part::Joined => {
            ledger_page(pool, &audit, part, after.as_deref(), scope.limit).await?
        }
        Part::FromOrphans | Part::JoinedOrphans => {
            orphan_page(pool, &audit, part, after.as_deref(), scope.limit).await?
        }
        Part::Duplicates => duplicate_page(pool, &audit, after.as_deref(), scope.limit).await?,
    };
    Ok(ComparePass {
        checked_through,
        rows_compared: pass.rows_compared,
        next_after: next_cursor(part, pass.page_end),
        divergences: pass.divergences,
        uncertain: pass.uncertain,
    })
}

/// The type of each column of `table` as a cast names it: the apply's own
/// rendering of a column's type (`type_sql`, ADR-0020), with no modifier.
async fn column_types(pool: &Pool, table: &str) -> Result<HashMap<String, String>, SelfCheckError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "select a.attname::text, {} \
                 from pg_catalog.pg_attribute a \
                 where a.attrelid = pg_catalog.to_regclass($1) and a.attnum > 0 \
                   and not a.attisdropped",
                crate::defs::copy_type::type_sql("a.atttypid")
            ),
            &[&ddl::regclass_arg(table)],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// The join's `JoinKind` of `def`.
fn join_kind(def: &TransformDef) -> JoinKind {
    spec::spec(def)
        .map(|join| join.kind)
        .expect("a 1-1 JOIN definition has a join")
}

/// Reads what the audit needs of `def`: both sides, their ledgers, and the
/// fields except the paused ones.
async fn build<'a>(
    pool: &Pool,
    def: &'a Definition,
    pk: &[PrimaryKeyColumn],
    paused: &HashSet<String>,
) -> Result<Audit<'a>, SelfCheckError> {
    let joined = {
        let client = pool.get().await?;
        catalog::join_table_of(&**client, def.id).await?
    }
    .ok_or_else(|| SelfCheckError::UnsupportedExpr {
        target: def.def.target.clone(),
        detail: "a 1-1 JOIN with no joined table recorded".to_string(),
    })?;
    let b_pk = ddl::source_primary_key(pool, &joined).await?;
    let schema_name = pool.schema();
    let side = |name: JoinSide,
                table: &str,
                pk: Vec<PrimaryKeyColumn>,
                hidden: HiddenTable,
                tag: &'static str| Side {
        ledger_name: hidden.name(def.id),
        tag,
        table: table.to_string(),
        ident: ddl::qualified_source_table(table),
        pk,
        ledger: LedgerRef::hidden(schema_name, def.id, hidden, &def.target_table),
        columns: spec::side_columns(&def.def, name).into_iter().collect(),
        key: spec::join_key(&def.def, name),
    };
    let a = side(
        JoinSide::From,
        &def.source_table,
        pk.to_vec(),
        HiddenTable::FromEntries,
        "a",
    );
    let b = side(
        JoinSide::Joined,
        &joined,
        b_pk,
        HiddenTable::JoinedEntries,
        "b",
    );
    let types = column_types(pool, &def.target_table).await?;
    let mut fields = Vec::new();
    for f in def.def.fields.iter().filter(|f| !paused.contains(&f.name)) {
        let sql = render(&def.def, &f.expr, "a", "b").map_err(|detail| {
            SelfCheckError::UnsupportedExpr {
                target: def.def.target.clone(),
                detail,
            }
        })?;
        let sql = match types.get(&f.name) {
            Some(ty) => format!("({sql})::{ty}"),
            None => sql,
        };
        fields.push(Field {
            name: f.name.clone(),
            sql,
        });
    }
    Ok(Audit {
        def,
        left: join_kind(&def.def) == JoinKind::Left,
        a,
        b,
        fields,
    })
}

/// A page of the statements' rows `(side, page_end, key, values)` as the two
/// sides' pages, the keys the page can't vouch for, and where it ends.
fn read_pages(rows: &[tokio_postgres::Row]) -> (Page, Page, Uncertain, Option<String>) {
    let (mut one, mut two) = (Page::new(), Page::new());
    let mut uncertain = Uncertain::default();
    let mut page_end = None;
    for row in rows {
        page_end = row.get(1);
        let key: String = row.get(2);
        match row.get::<_, i32>(0) {
            3 => {
                uncertain.keys.insert(key);
            }
            4 => uncertain.all = true,
            side => {
                let values: Vec<Option<String>> = row.get(3);
                if side == 1 {
                    one.insert(key, values);
                } else {
                    two.insert(key, values);
                }
            }
        }
    }
    (one, two, uncertain, page_end)
}

/// A page of the target part.
async fn target_page(
    pool: &Pool,
    audit: &Audit<'_>,
    pk: &[PrimaryKeyColumn],
    after: Option<&str>,
    limit: i64,
) -> Result<Pass, SelfCheckError> {
    let client = pool.get().await?;
    let mut keyset = Keyset::resolve(&client, pk).await?;
    keyset.set_cursor(after)?;
    let sql = audit.target_sql(&keyset);
    let rows = client.query(sql.as_str(), &[&limit]).await?;
    let (recomputed, persisted, uncertain, page_end) = read_pages(&rows);
    let columns: Vec<&str> = audit.fields.iter().map(|f| f.name.as_str()).collect();
    let diff = diff_page(&columns, recomputed, persisted, page_end);
    Ok(Pass {
        rows_compared: diff.rows_compared,
        page_end: diff.next_after,
        divergences: diff.divergences,
        uncertain,
    })
}

/// A cell of `side`'s ledger on `key`.
fn ledger_cell(
    side: &Side,
    key: &str,
    column: &str,
    persisted: Option<String>,
    recomputed: Option<String>,
) -> Divergence {
    Divergence::Cell {
        key: key.to_string(),
        column: side.divergence_column(column),
        persisted,
        recomputed,
    }
}

/// A page of a ledger's part against its source (`Part::From`, `Part::Joined`).
async fn ledger_page(
    pool: &Pool,
    audit: &Audit<'_>,
    part: Part,
    after: Option<&str>,
    limit: i64,
) -> Result<Pass, SelfCheckError> {
    let side = if part == Part::From {
        &audit.a
    } else {
        &audit.b
    };
    let client = pool.get().await?;
    let mut keyset = Keyset::resolve(&client, &side.pk).await?;
    keyset.set_cursor(after)?;
    let sql = audit.ledger_sql(side, &keyset);
    let rows = client.query(sql.as_str(), &[&limit]).await?;
    let (expected, persisted, uncertain, page_end) = read_pages(&rows);
    let mut divergences = Vec::new();
    for (key, values) in &expected {
        let Some(held) = persisted.get(key) else {
            divergences.push(ledger_cell(side, key, ENTRY, None, Some(LIVE.to_string())));
            continue;
        };
        // The values lead with `LIVE`, which both sides hold.
        for (column, (want, got)) in side
            .columns
            .iter()
            .zip(values.iter().skip(1).zip(held.iter().skip(1)))
        {
            if want != got {
                divergences.push(ledger_cell(side, key, column, got.clone(), want.clone()));
            }
        }
    }
    Ok(Pass {
        rows_compared: expected.len() as i64,
        page_end,
        divergences,
        uncertain,
    })
}

/// A page of a ledger's live entries, looked up in the source
/// (`Part::FromOrphans`, `Part::JoinedOrphans`).
async fn orphan_page(
    pool: &Pool,
    audit: &Audit<'_>,
    part: Part,
    after: Option<&str>,
    limit: i64,
) -> Result<Pass, SelfCheckError> {
    let side = audit.side(part);
    let client = pool.get().await?;
    let ledger = &side.ledger.ident;
    let key_col = quote_ident(schema::KEY_COLUMN);
    let tomb = quote_ident(schema::TOMBSTONE_COLUMN);
    let keys: Vec<String> = match after {
        Some(after) => {
            client
                .query(
                    &format!(
                        "select {key_col} from {ledger} where not {tomb} and {key_col} > $1 \
                         order by {key_col} limit $2"
                    ),
                    &[&after, &limit],
                )
                .await?
        }
        None => {
            client
                .query(
                    &format!(
                        "select {key_col} from {ledger} where not {tomb} \
                         order by {key_col} limit $1"
                    ),
                    &[&limit],
                )
                .await?
        }
    }
    .iter()
    .map(|row| row.get(0))
    .collect();
    let page_end = (keys.len() as i64 == limit)
        .then(|| keys.last().cloned())
        .flatten();
    if keys.is_empty() {
        return Ok(Pass {
            rows_compared: 0,
            page_end: None,
            divergences: Vec::new(),
            uncertain: Uncertain::default(),
        });
    }
    // Each key as its primary key's columns, so the source is probed by its
    // index.
    let mut objects = Vec::with_capacity(keys.len());
    for key in &keys {
        let parts = ddl::split_pk_key(&side.pk, &side.table, key)?;
        let object: serde_json::Map<String, serde_json::Value> = side
            .pk
            .iter()
            .zip(parts)
            .map(|(column, part)| {
                (
                    column.name.clone(),
                    part.map_or(serde_json::Value::Null, |p| {
                        serde_json::Value::String(p.into_owned())
                    }),
                )
            })
            .collect();
        objects.push(serde_json::Value::Object(object).to_string());
    }
    let rows = client
        .query(audit.orphans_sql(side).as_str(), &[&keys, &objects])
        .await?;
    let mut divergences = Vec::new();
    let mut uncertain = Uncertain::default();
    for row in &rows {
        let key: String = row.get(1);
        match row.get::<_, i32>(0) {
            3 => {
                uncertain.keys.insert(key);
            }
            4 => uncertain.all = true,
            _ => divergences.push(ledger_cell(side, &key, ENTRY, Some(LIVE.to_string()), None)),
        }
    }
    divergences.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    Ok(Pass {
        rows_compared: keys.len() as i64,
        page_end,
        divergences,
        uncertain,
    })
}

/// A page of the join keys that hold more than one live `M` entry: the next
/// `limit` live entries with a key in the key's order, and each key among
/// them counted over all of `M`, so a key whose entries straddle the page's
/// end is counted whole on the page it starts on. A page reads `limit`
/// entries whether or not any key is duplicated.
async fn duplicate_page(
    pool: &Pool,
    audit: &Audit<'_>,
    after: Option<&str>,
    limit: i64,
) -> Result<Pass, SelfCheckError> {
    let side = &audit.b;
    let client = pool.get().await?;
    let m = &side.ledger.ident;
    let mut keyset = Keyset::resolve_columns(&client, m, &side.key).await?;
    keyset.set_cursor(after)?;
    let refs = keyset.refs(None);
    let carried = keyset.select_carried(&refs);
    let order = keyset.order_by(&refs);
    let key = ddl::pk_key_sql_expr(keyset.columns(), None);
    let tomb = quote_ident(schema::TOMBSTONE_COLUMN);
    let not_null: Vec<String> = refs.iter().map(|r| format!("{r} is not null")).collect();
    let entries = keyset.side(&refs, |cond| {
        format!(
            "select {key} as __k{carried} from {m} \
             where {cond} and not {tomb} and {not_null} order by {order} limit $1",
            not_null = not_null.join(" and "),
        )
    });
    let carried_columns = keyset.carried(None);
    let same: Vec<String> = refs
        .iter()
        .zip(keyset.carried(Some("g")))
        .map(|(r, c)| format!("m.{r} = {c}"))
        .collect();
    let sql = format!(
        "with entries as materialized ( \
             select e.*, row_number() over (order by {carried_order}) as __rn \
             from ({entries}) e), \
         groups as ( \
             select min(__k) as __k, min(__rn) as __rn, {carried_list} \
             from entries group by {carried_list}) \
         select 0::int, null::text, count(*)::text, \
                (select __k from entries order by __rn desc limit 1), 0::bigint \
         from entries \
         union all \
         select 1, g.__k, c.n::text, null, g.__rn from groups g \
         cross join lateral (select count(*) as n from {m} m \
                             where {same} and not m.{tomb}) c \
         where c.n > 1",
        carried_order = keyset.order_by(&carried_columns),
        carried_list = carried_columns.join(", "),
        same = same.join(" and "),
    );
    let rows = client.query(sql.as_str(), &[&limit]).await?;
    let mut entries_read = 0;
    let mut last = None;
    let mut found = Vec::new();
    for row in &rows {
        match row.get::<_, i32>(0) {
            0 => {
                entries_read = row.get::<_, String>(2).parse::<i64>().unwrap_or(0);
                last = row.get::<_, Option<String>>(3);
            }
            _ => found.push((
                row.get::<_, i64>(4),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
            )),
        }
    }
    found.sort();
    let divergences: Vec<Divergence> = found
        .into_iter()
        .map(|(_, key, count)| Divergence::Cell {
            key,
            column: side.divergence_column("live_entries"),
            persisted: Some(count),
            recomputed: Some("1".to_string()),
        })
        .collect();
    let page_end = (entries_read == limit).then_some(last).flatten();
    // A hand-off of a join key between two `b` rows holds two entries on it
    // while its two changes are applied out of order: what is found is the
    // ring's, not `M`'s, while `b`'s ring holds an unapplied row.
    let unapplied = if divergences.is_empty() {
        false
    } else {
        let ring = side.ring(audit.id());
        let ctes = ring.one_to_one_ctes(&side.ledger);
        client
            .query_one(
                &format!(
                    "with {} select {}",
                    ctes.trim_end().trim_end_matches(','),
                    ring.unapplied_sql()
                ),
                &[],
            )
            .await?
            .get(0)
    };
    Ok(Pass {
        rows_compared: entries_read,
        page_end,
        divergences,
        uncertain: Uncertain {
            all: unapplied,
            keys: BTreeSet::new(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_names_its_part_and_key() {
        for part in Part::ALL {
            let bare = parse_cursor(Some(part.tag()));
            assert_eq!(bare, (part, None));
            let keyed = parse_cursor(Some(&format!("{}:a:b", part.tag())));
            assert_eq!(keyed, (part, Some("a:b".to_string())));
            assert_eq!(
                next_cursor(part, Some("k".to_string())),
                Some(format!("{}:k", part.tag()))
            );
        }
        assert_eq!(parse_cursor(None), (Part::Target, None));
        assert_eq!(
            parse_cursor(Some("17")),
            (Part::Target, Some("17".to_string()))
        );
        assert_eq!(next_cursor(Part::Target, None), Some("r".to_string()));
        assert_eq!(next_cursor(Part::Duplicates, None), None);
    }

    #[test]
    fn the_walk_visits_every_part_once_in_order() {
        let mut part = Part::Target;
        let mut seen = vec![part];
        while let Some(next) = part.next() {
            seen.push(next);
            part = next;
        }
        assert_eq!(seen, Part::ALL);
    }
}
