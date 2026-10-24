//! The type matrix (ADR-0020, #1289): every type with a modifier, under every
//! kind of change to its source column, in every place Trellis keeps a copy
//! of it, ends exact, or paused with a reason and exact after `RESUME`.
//!
//! The seam it owns is the type of the columns Trellis creates from a source
//! column and the values it casts to them. A copy is the source column's type
//! with no modifier, so a change of the source's modifier that fires no
//! trigger (`timestamp(0)` to `timestamp(3)`, `numeric(6,2)` to `numeric`)
//! reaches every copy exactly: Postgres rounds silently on plain assignment
//! into `numeric(p,s)`, `timestamp(p)`, `timestamptz(p)`, `time(p)`,
//! `timetz(p)` and `interval` columns, so only an unmodified column protects
//! one (#1288, from the research of #1224).
//!
//! **Axes.**
//! - The type ([`TYPES`]): `varchar`, `bpchar`, `bit`, `varbit`, `numeric`,
//!   `time`, `timetz`, `timestamp`, `timestamptz`, `interval` and an array
//!   (refused in every place), `char(n)` moved to `text`, and an enum, whose
//!   value identity is its labels, relabelled or re-typed to another enum
//!   (#1327), which pauses every reader in every place.
//! - The change ([`Change`]): a catalog-only widening, a widening or `USING`
//!   rewrite that re-renders the stored values, a narrowing, and an enum
//!   relabel (`RENAME VALUE`), which pauses every reader of the type in every
//!   place (#1241).
//! - The place ([`Place`]): a 1-1 primary key and passthrough, a group-basis
//!   `GROUP BY` key, a `JOIN`'s `ON` columns and its `GROUP BY` key. The two
//!   `JOIN` places keep their columns in the hidden tables `P`, `M`, `J` and
//!   `T`. The pairing axis is [`Place::JoinOnOneSided`]: only one table's
//!   `ON` column changes, so a catalog-only widening pauses nothing (ADR-0020
//!   rule 6).
//!
//! A cell whose type the place refuses at define is skipped by [`admits`], and
//! [`a_type_a_place_refuses_is_refused`] holds that table to the live
//! validator, so a type that becomes admissible is a cell that has to be
//! filled in.
//!
//! **Outcome.** A captured write before the change is exact. A catalog-only
//! widening is exact with no pause at all, before and after the capture pass
//! ([`Change::CatalogOnly`]). The others are exact once the definition has
//! converged: nothing may be left diverged, and a pause must carry a reason,
//! after which `RESUME` rebuilds exactly. A passthrough never fails a write
//! or pauses. Every step is an explicit pass or drain; nothing polls for
//! convergence (#297).
//!
//! **Not in the matrix** (each is a later step of #1289, or has its own test):
//! - a Trellis copy against a source column in a `JOIN`'s `ON` has its own
//!   tests: [`a_trellis_copy_pairs_with_a_source_column_of_any_modifier`] at
//!   define, [`a_trellis_copy_joins_a_modified_source_column_end_to_end`] as
//!   rows flow;
//! - the rename row: a rename after define is `capture_type_rename.rs`'s, and
//!   one before a baseline records it is step 3;
//! - integer and float widths, the only changes that re-type a copy
//!   (`capture_key_retype.rs`);
//! - the ledger `GROUP BY` path, whose copies are family-typed like the
//!   group-basis ones and which needs the group-basis knob off.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::Driver;
use testkit::TestCluster;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{PgType, ValueType, chunk_queue};
use trellis::integer::IntWidth;
use trellis::staging::apply;
use trellis::staging::self_check::CheckPageExt;
use trellis::{Config, SelfCheckMode, Trellis, TrellisOptions};

/// The kind of change to a source column.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Change {
    /// A widening that changes only the catalog: nothing is rewritten, no
    /// trigger fires, and no stored value changes.
    CatalogOnly,
    /// A widening, or a `USING` expression, that rewrites the table and
    /// re-renders the values stored.
    Rewrite,
    /// A narrowing that rounds or truncates the values stored.
    Narrow,
    /// An enum value relabelled (`ALTER TYPE ... RENAME VALUE`): no column
    /// changes, but the label text Trellis stored names a value that no
    /// longer exists (#1241). It pauses every reader of the type, and
    /// `RESUME` rebuilds.
    Relabel,
}

/// Where Trellis keeps a copy of the column.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Place {
    /// A 1-1 target's bare passthrough field.
    Passthrough,
    /// A 1-1 target's primary key.
    PrimaryKey,
    /// A group-basis aggregate's `GROUP BY` key.
    GroupBy,
    /// A `JOIN`'s `ON` columns (both tables), also its `GROUP BY` key.
    JoinOn,
    /// A `JOIN`'s `GROUP BY` key on one table, joined on an integer.
    JoinGroupBy,
    /// The pairing axis (ADR-0020 rule 6): a `JOIN`'s `ON` columns, of which
    /// only the left table's column changes, so the pair is one-sided.
    JoinOnOneSided,
}

const PLACES: [Place; 6] = [
    Place::Passthrough,
    Place::PrimaryKey,
    Place::GroupBy,
    Place::JoinOn,
    Place::JoinGroupBy,
    Place::JoinOnOneSided,
];

/// One change of a type: the source column starts as `start` holding `seed`,
/// is altered with `type {alter}` (`{c}` is the column's name, for a
/// `USING`), and then takes `hi`, a value only the altered type can hold or
/// that is new to it.
struct Row {
    start: &'static str,
    alter: &'static str,
    seed: &'static str,
    hi: &'static str,
}

/// One type with a modifier and its rows. `None` when the type has no change
/// of that kind.
struct Ty {
    label: &'static str,
    value_type: ValueType,
    /// DDL the source's needs first (the enum type).
    prelude: &'static str,
    catalog_only: Option<Row>,
    rewrite: Option<Row>,
    narrow: Option<Row>,
    /// For [`Change::Relabel`], `alter` is the whole statement.
    relabel: Option<Row>,
}

impl Ty {
    fn row(&self, change: Change) -> Option<&Row> {
        match change {
            Change::CatalogOnly => self.catalog_only.as_ref(),
            Change::Rewrite => self.rewrite.as_ref(),
            Change::Narrow => self.narrow.as_ref(),
            Change::Relabel => self.relabel.as_ref(),
        }
    }
}

const fn row(
    start: &'static str,
    alter: &'static str,
    seed: &'static str,
    hi: &'static str,
) -> Option<Row> {
    Some(Row {
        start,
        alter,
        seed,
        hi,
    })
}

/// Every type with a modifier. A `Rewrite` row forces the table rewrite with
/// a `USING` expression where Postgres would not otherwise rewrite.
const TYPES: &[Ty] = &[
    Ty {
        label: "varchar",
        value_type: ValueType::Text,
        prelude: "",
        catalog_only: row("varchar(5)", "varchar(8)", "abc", "abcdefg"),
        rewrite: row(
            "varchar(5)",
            "varchar(8) using ({c}::text || '')::varchar(8)",
            "abc",
            "abcdefg",
        ),
        narrow: row("varchar(8)", "varchar(3) using left({c}, 3)", "abc", "xyz"),
        relabel: None,
    },
    Ty {
        label: "bpchar",
        value_type: ValueType::Text,
        prelude: "",
        catalog_only: None,
        rewrite: row("char(5)", "char(8)", "abc", "abcdefg"),
        narrow: row(
            "char(8)",
            "char(3) using left({c}, 3)::char(3)",
            "abc",
            "xyz",
        ),
        relabel: None,
    },
    // Not a modifier change: `char(n)` to `text` is #1238's other shape.
    Ty {
        label: "bpchar to text",
        value_type: ValueType::Text,
        prelude: "",
        catalog_only: None,
        rewrite: row("char(5)", "text", "abc", "abcdefg"),
        narrow: None,
        relabel: None,
    },
    Ty {
        label: "bit",
        value_type: ValueType::Other(PgType::Bit),
        prelude: "",
        catalog_only: None,
        rewrite: row("bit(5)", "bit(8) using {c}::bit(8)", "10101", "11111111"),
        narrow: row("bit(8)", "bit(3) using {c}::bit(3)", "10101000", "110"),
        relabel: None,
    },
    Ty {
        label: "varbit",
        value_type: ValueType::Other(PgType::VarBit),
        prelude: "",
        catalog_only: row("bit varying(5)", "bit varying(8)", "10101", "10101000"),
        rewrite: row(
            "bit varying(5)",
            "bit varying(8) using ({c}::text)::bit varying(8)",
            "10101",
            "10101000",
        ),
        narrow: row(
            "bit varying(8)",
            "bit varying(3) using {c}::bit varying(3)",
            "101",
            "110",
        ),
        relabel: None,
    },
    Ty {
        label: "numeric",
        value_type: ValueType::Numeric,
        prelude: "",
        catalog_only: row("numeric(6,2)", "numeric", "1.25", "1.23456789"),
        rewrite: row("numeric(6,2)", "numeric(8,4)", "1.25", "1.2345"),
        narrow: row("numeric(8,4)", "numeric(6,1)", "1.2000", "2.5"),
        relabel: None,
    },
    Ty {
        label: "time",
        value_type: ValueType::Other(PgType::Time),
        prelude: "",
        catalog_only: row("time(0)", "time(3)", "10:00:00", "10:00:00.4"),
        rewrite: None,
        narrow: row("time(3)", "time(0)", "10:00:00", "11:00:00"),
        relabel: None,
    },
    Ty {
        label: "timetz",
        value_type: ValueType::Other(PgType::TimeTz),
        prelude: "",
        catalog_only: row("timetz(0)", "timetz(3)", "10:00:00+00", "10:00:00.4+00"),
        rewrite: None,
        narrow: row("timetz(3)", "timetz(0)", "10:00:00+00", "11:00:00+00"),
        relabel: None,
    },
    Ty {
        label: "timestamp",
        value_type: ValueType::Other(PgType::Timestamp),
        prelude: "",
        catalog_only: row(
            "timestamp(0)",
            "timestamp(3)",
            "2024-01-01 00:00:00",
            "2024-01-01 00:00:00.4",
        ),
        rewrite: None,
        narrow: row(
            "timestamp(3)",
            "timestamp(0)",
            "2024-01-01 00:00:00",
            "2024-01-02 00:00:00",
        ),
        relabel: None,
    },
    Ty {
        label: "timestamptz",
        value_type: ValueType::Other(PgType::TimestampTz),
        prelude: "",
        catalog_only: row(
            "timestamptz(0)",
            "timestamptz(3)",
            "2024-01-01 00:00:00+00",
            "2024-01-01 00:00:00.4+00",
        ),
        rewrite: None,
        narrow: row(
            "timestamptz(3)",
            "timestamptz(0)",
            "2024-01-01 00:00:00+00",
            "2024-01-02 00:00:00+00",
        ),
        relabel: None,
    },
    Ty {
        label: "interval",
        value_type: ValueType::Other(PgType::Interval),
        prelude: "",
        catalog_only: row(
            "interval(0)",
            "interval(3)",
            "1 day 00:00:01",
            "1 day 00:00:01.4",
        ),
        rewrite: None,
        narrow: row(
            "interval(3)",
            "interval(0)",
            "1 day 00:00:01",
            "2 days 00:00:01",
        ),
        relabel: None,
    },
    Ty {
        label: "interval fields",
        value_type: ValueType::Other(PgType::Interval),
        prelude: "",
        catalog_only: row(
            "interval day",
            "interval day to second",
            "1 day",
            "1 day 00:00:01.5",
        ),
        rewrite: None,
        narrow: None,
        relabel: None,
    },
    Ty {
        label: "varchar[]",
        value_type: ValueType::Other(PgType::Unrecognized),
        prelude: "",
        catalog_only: None,
        rewrite: row("varchar(5)[]", "varchar(8)[]", "{abc}", "{abcdefg}"),
        narrow: None,
        relabel: None,
    },
    // No modifier: its value identity is its labels (ADR-0020, #1241).
    Ty {
        label: "enum",
        value_type: ValueType::Other(PgType::Enum("enum:public.tone")),
        prelude: "create type public.tone as enum ('brisk', 'calm'); ",
        catalog_only: None,
        rewrite: None,
        narrow: None,
        relabel: row(
            "public.tone",
            "alter type public.tone rename value 'brisk' to 'zippy'",
            "brisk",
            // The seed is `zippy` now, a key in some places.
            "calm",
        ),
    },
    // A re-type from one enum to another (#1327): a value-identity change
    // like a relabel. The new type shares no label with the old one, so no
    // stored label casts to it; the source's own `USING` maps the values.
    Ty {
        label: "enum to enum",
        value_type: ValueType::Other(PgType::Enum("enum:public.tone")),
        prelude: "create type public.tone as enum ('brisk', 'calm'); \
                  create type public.mood as enum ('fast', 'slow'); ",
        catalog_only: None,
        rewrite: row(
            "public.tone",
            "public.mood using (case {c} when 'brisk' then 'fast' else 'slow' end)::public.mood",
            "brisk",
            "slow",
        ),
        narrow: None,
        relabel: None,
    },
];

/// Whether `place` admits a column of the type `label` at define. The table
/// is held to the validator by [`a_type_a_place_refuses_is_refused`].
fn admits(place: Place, label: &str) -> bool {
    match place {
        // A passthrough takes any type the registry places. An array it
        // doesn't place is out of the definition.
        Place::Passthrough => label != "varchar[]",
        // An enum is a key, join and group key in every place.
        _ if label.starts_with("enum") => true,
        // A key is a text-stable type: not `numeric`, `interval`,
        // `character(n)` or an array.
        Place::PrimaryKey => matches!(
            label,
            "varchar" | "bit" | "varbit" | "time" | "timetz" | "timestamp" | "timestamptz"
        ),
        // A `JOIN` groups by its `ON` column, and a fixed-length `bit` is no
        // group key.
        Place::JoinOn | Place::JoinOnOneSided => matches!(
            label,
            "varchar" | "varbit" | "time" | "timetz" | "timestamp" | "timestamptz"
        ),
        // A group key takes the family type: `bpchar` and `numeric` are
        // admitted, and a fixed-length `bit` is not.
        Place::GroupBy | Place::JoinGroupBy => matches!(
            label,
            "varchar"
                | "bpchar"
                | "bpchar to text"
                | "varbit"
                | "numeric"
                | "time"
                | "timetz"
                | "timestamp"
                | "timestamptz"
        ),
    }
}

/// What Trellis reported after one cell.
struct Outcome {
    /// How many drain attempts failed before a page committed (a copy that
    /// refuses a value errors the drain and charges the key a death).
    drain_failures: usize,
    /// The status and `self_check` verdict after the drain and before any
    /// pass.
    status_before_pass: String,
    self_check_before_pass: String,
    /// Rows of the target that differ from the source before any pass.
    diverged_before_pass: Vec<String>,
    /// The first pass's status, with what differs, before any `RESUME`.
    status_after_pass: String,
    diverged_after_pass: Vec<String>,
    /// Every pause the definition went through to converge, each a reason.
    pauses: Vec<String>,
    /// The status each round of the convergence loop found.
    trace: Vec<String>,
    /// The end state, once converged.
    status: String,
    self_check: String,
    diverged: Vec<String>,
}

async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(
        &mut d.ctl,
        &pool,
        DEFAULT_SCHEMA,
        "type_matrix_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

async fn facade(dsn: &str) -> Trellis {
    let config = Config::with_schema(dsn.to_string(), DEFAULT_SCHEMA).expect("config");
    Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect")
}

async fn status_of(trellis: &Trellis, target: &str) -> String {
    let reported = trellis
        .status(target)
        .await
        .expect("status")
        .expect("the definition");
    let held = match &reported.held_keys {
        Some(held) => format!(" (held keys: {})", held.count),
        None => String::new(),
    };
    match reported.capture_failure {
        Some(failure) => format!("{:?}: {}{held}", reported.status, failure.error),
        None => format!("{:?}{held}", reported.status),
    }
}

/// `self_check`'s verdict. Nothing drains in the background, so whether the
/// target has caught up is settled before the call: the timeout only bounds
/// how long a held key (which never catches up) takes to report
/// `NotCaughtUp`.
async fn self_check_of(trellis: &Trellis, target: &str) -> String {
    match trellis
        .self_check_to_end(target, SelfCheckMode::Strict, Duration::from_secs(3))
        .await
    {
        Ok(report) => format!("{:?}", report.outcome),
        Err(err) => format!("error: {err}"),
    }
}

/// The rows that differ between the target's and the oracle's (the source's)
/// shape of `target_sql`.
async fn diverged(d: &Driver, target_sql: &str, source_sql: &str) -> Vec<String> {
    d.rows(&format!(
        "(({target_sql}) except ({source_sql})) union all (({source_sql}) except ({target_sql}))"
    ))
    .await
}

/// Seals and drains eight times, tolerating a drain that fails because a
/// copy refuses a value: each failure charges the key a death, so the page
/// commits once the key is held. A fixed number of rounds with no sleep and
/// no exit on a condition, so it is not a wait for convergence (#297).
/// Returns how many attempts failed.
async fn settle_tolerant(d: &mut Driver) -> usize {
    let mut failures = 0;
    for _ in 0..8 {
        let batch = d.seal().await;
        loop {
            match apply::drain_once(d.pool(), batch, "worker", 1, drain_driver::WAKE).await {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(err) => {
                    failures += 1;
                    assert!(failures <= 40, "the batch never drained: {err}");
                }
            }
        }
        d.retire().await;
    }
    failures
}

/// Claims, runs and finishes every pending backfill chunk, then settles the
/// builds.
async fn run_backfill_chunks(pool: &trellis::Pool) {
    const CLAIMED_BY: &str = "type_matrix_backfill";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            trellis::staging::build::settle_builds(pool).await;
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                CLAIMED_BY,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// One cell: a source, one definition named `c`, a seed, an alter, a write,
/// and the SQL that reads the target and its oracle (the source) in the same
/// shape.
struct Scenario {
    ddl: String,
    columns: Vec<(&'static str, ValueType)>,
    definition: &'static str,
    captured: &'static [&'static str],
    seed_sql: String,
    alter_sql: String,
    hi_sql: String,
    target_sql: String,
    oracle_sql: String,
}

/// `row`'s alter clause for `column`.
fn alter_clause(row: &Row, column: &str) -> String {
    row.alter.replace("{c}", column)
}

fn scenario(place: Place, ty: &Ty, row: &Row) -> Scenario {
    let value_type = ty.value_type;
    // A group key compares by value, as a `GROUP BY` does. An enum's is its
    // label, and its text compares a target re-typed from another enum
    // with the source before a `RESUME` re-types it.
    let g = if ty.label.starts_with("enum") {
        "g::text"
    } else {
        "g"
    };
    let alter = |table: &str, column: &str| {
        format!(
            "alter table public.{table} alter column {column} type {}",
            alter_clause(row, column)
        )
    };
    match place {
        // A 1-1 target over `t(id integer primary key, v <T>, n integer)`
        // copying `v` as a passthrough.
        Place::Passthrough => Scenario {
            ddl: format!(
                "create table public.t (id integer primary key, v {}, n integer)",
                row.start
            ),
            columns: vec![("id", int4()), ("v", value_type), ("n", int4())],
            definition: "TRANSFORM c FROM public.t SELECT v AS v, n AS n",
            captured: &["public.t"],
            seed_sql: format!("insert into public.t values (1, '{}', 1)", row.seed),
            alter_sql: alter("t", "v"),
            hi_sql: format!("insert into public.t values (2, '{}', 2)", row.hi),
            target_sql: "select id, v::text, n from public.c".into(),
            oracle_sql: "select id, v::text, n from public.t".into(),
        },
        // A 1-1 target over `t(k <T> primary key, n integer)`, keyed by `k`.
        Place::PrimaryKey => Scenario {
            ddl: format!(
                "create table public.t (k {} primary key, n integer)",
                row.start
            ),
            columns: vec![("k", value_type), ("n", int4())],
            definition: "TRANSFORM c FROM public.t SELECT n AS n",
            captured: &["public.t"],
            seed_sql: format!("insert into public.t values ('{}', 1)", row.seed),
            alter_sql: alter("t", "k"),
            hi_sql: format!("insert into public.t values ('{}', 2)", row.hi),
            target_sql: "select k::text, n from public.c".into(),
            oracle_sql: "select k::text, n from public.t".into(),
        },
        // A group-basis aggregate over `t(id integer primary key, g <T>, x
        // integer)` grouped by `g`. The key compares by value: a group row
        // holds one of the equal values' texts, as a `GROUP BY` does
        // (`1.25` beside the source's `1.2500`, ADR-0020).
        Place::GroupBy => Scenario {
            ddl: format!(
                "create table public.t (id integer primary key, g {}, x integer); \
                 create index on public.t (g)",
                row.start
            ),
            columns: vec![("id", int4()), ("g", value_type), ("x", int4())],
            definition: "TRANSFORM c FROM public.t GROUP BY g \
                        WITHOUT SOURCE INDEX SELECT g AS g, COUNT(*) AS n, SUM(x) AS sx",
            captured: &["public.t"],
            seed_sql: format!("insert into public.t values (1, '{}', 1)", row.seed),
            alter_sql: alter("t", "g"),
            hi_sql: format!("insert into public.t values (2, '{}', 2)", row.hi),
            target_sql: format!("select {g}, n, sx from public.c"),
            oracle_sql: format!(
                "select {g}, count(*)::bigint, sum(x)::bigint from public.t group by g"
            ),
        },
        // A `JOIN` on `a.k = b.k` of two tables whose `k` is `<T>`, grouped
        // by `k`. Both tables change `k`.
        Place::JoinOn => Scenario {
            ddl: format!(
                "create table public.a (id integer primary key, k {0}, x integer); \
                 create table public.b (id integer primary key, k {0}, y integer); \
                 create index on public.a (k); create index on public.b (k)",
                row.start
            ),
            columns: vec![
                ("id", int4()),
                ("k", value_type),
                ("x", int4()),
                ("y", int4()),
            ],
            definition: "TRANSFORM c FROM public.a LEFT JOIN public.b ON a.k = b.k \
                         GROUP BY a.k WITHOUT SOURCE INDEX SELECT a.k AS k, COUNT(*) AS n, SUM(b.y) AS sy",
            captured: &["public.a", "public.b"],
            seed_sql: format!(
                "insert into public.a values (1, '{0}', 1); \
                 insert into public.b values (1, '{0}', 10)",
                row.seed
            ),
            alter_sql: format!("{}; {}", alter("a", "k"), alter("b", "k")),
            hi_sql: format!(
                "insert into public.a values (2, '{0}', 2); \
                 insert into public.b values (2, '{0}', 20)",
                row.hi
            ),
            target_sql: "select k::text, n, sy from public.c".into(),
            oracle_sql: "select a.k::text, count(*)::bigint, sum(b.y)::bigint \
                         from public.a left join public.b on a.k = b.k group by a.k"
                .into(),
        },
        // The pairing axis: the same `JOIN` as `JoinOn`, but only `a.k`
        // changes, so the pair's two columns differ in modifier.
        Place::JoinOnOneSided => Scenario {
            alter_sql: alter("a", "k"),
            // `b.k` keeps its type, so it takes the seed; `a.k` takes the
            // value only the altered type holds.
            hi_sql: format!(
                "insert into public.a values (2, '{}', 2); \
                 insert into public.b values (2, '{}', 20)",
                row.hi, row.seed
            ),
            ..scenario(Place::JoinOn, ty, row)
        },
        // A `JOIN` on an integer, grouped by a `<T>` column of the left
        // table, the key compared by value as `GroupBy`'s.
        Place::JoinGroupBy => Scenario {
            ddl: format!(
                "create table public.a (id integer primary key, g {}, j integer); \
                 create table public.b (id integer primary key, j integer, y integer); \
                 create index on public.a (j); create index on public.b (j)",
                row.start
            ),
            columns: vec![
                ("id", int4()),
                ("g", value_type),
                ("j", int4()),
                ("y", int4()),
            ],
            definition: "TRANSFORM c FROM public.a LEFT JOIN public.b ON a.j = b.j \
                         GROUP BY a.g WITHOUT SOURCE INDEX SELECT a.g AS g, COUNT(*) AS n, SUM(b.y) AS sy",
            captured: &["public.a", "public.b"],
            seed_sql: format!(
                "insert into public.a values (1, '{}', 1); insert into public.b values (1, 1, 10)",
                row.seed
            ),
            alter_sql: alter("a", "g"),
            hi_sql: format!(
                "insert into public.a values (2, '{}', 2); insert into public.b values (2, 2, 20)",
                row.hi
            ),
            target_sql: format!("select {g}, n, sy from public.c"),
            oracle_sql: format!(
                "select a.{g}, count(*)::bigint, sum(b.y)::bigint \
                 from public.a left join public.b on a.j = b.j group by a.g"
            ),
        },
    }
}

/// The columns of the tables Trellis created for the definition that have a
/// type modifier: none may (ADR-0020). The target is `public.c`; a `JOIN`'s
/// hidden tables are `join_<id>_*` in Trellis's schema.
async fn copies_with_a_modifier(d: &Driver) -> Vec<String> {
    d.rows(&format!(
        "select c.relname::text, a.attname::text, \
                pg_catalog.format_type(a.atttypid, a.atttypmod) \
         from pg_catalog.pg_attribute a \
         join pg_catalog.pg_class c on c.oid = a.attrelid and c.relkind = 'r' \
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
         where a.attnum > 0 and not a.attisdropped and a.atttypmod <> -1 \
           and ((n.nspname = 'public' and c.relname = 'c') \
                or (n.nspname = '{DEFAULT_SCHEMA}' and c.relname like 'join\\_%')) \
         order by 1, 2"
    ))
    .await
}

async fn run(place: Place, ty: &Ty, change: Change, row: &Row) -> Outcome {
    let scn = scenario(place, ty, row);
    trellis::dev::staging::set_group_basis(true);
    let ddl = format!("{}{}", ty.prelude, scn.ddl);
    let mut d = Driver::start(&ddl, &scn.columns, &[scn.definition], scn.captured).await;
    assert_eq!(
        copies_with_a_modifier(&d).await,
        Vec::<String>::new(),
        "{place:?}, {}: a column Trellis created has a type modifier",
        ty.label
    );
    write(&d, &scn.seed_sql).await;
    d.settle().await;
    // The seed is a captured write before any change: the apply's cast of
    // it to the copy's type must keep it whole (a bare `bit` is `bit(1)`,
    // #1262).
    assert_eq!(
        diverged(&d, &scn.target_sql, &scn.oracle_sql).await,
        Vec::<String>::new(),
        "{place:?}, {}: a captured write before any change differs",
        ty.label
    );
    // A relabel alters the type, not a column.
    let alter_sql = if change == Change::Relabel {
        row.alter.to_string()
    } else {
        scn.alter_sql.clone()
    };
    d.ctl.batch_execute(&alter_sql).await.expect("alter");
    write(&d, &scn.hi_sql).await;
    let drain_failures = settle_tolerant(&mut d).await;
    let diverged_before_pass = diverged(&d, &scn.target_sql, &scn.oracle_sql).await;
    let trellis = facade(d.db.dsn()).await;
    let status_before_pass = status_of(&trellis, "c").await;
    let self_check_before_pass = self_check_of(&trellis, "c").await;

    pass(&mut d).await;
    settle_tolerant(&mut d).await;
    let status_after_pass = status_of(&trellis, "c").await;
    let diverged_after_pass = diverged(&d, &scn.target_sql, &scn.oracle_sql).await;

    // Converge: explicit rounds of the builds a pass asks for, a drain and a
    // pass, resuming a pause. A change that is not a catalog-only widening
    // may pause (the rewrite, the narrowing), and `RESUME` rebuilds.
    let mut pauses = Vec::new();
    let mut trace = Vec::new();
    for _ in 0..8 {
        run_backfill_chunks(d.pool()).await;
        settle_tolerant(&mut d).await;
        let status = status_of(&trellis, "c").await;
        trace.push(status.clone());
        // A pause with a reason of its own is resumed once. A resume the
        // staging worker has yet to finish reads `resuming: …` (or, once its
        // request is done, no reason): the next pass finishes it.
        let own_reason = status
            .strip_prefix("Paused: ")
            .is_some_and(|why| !why.starts_with("resuming:"));
        if own_reason {
            pauses.push(status);
            trellis
                .apply("RESUME TRANSFORM c")
                .await
                .expect("resume the paused definition");
        } else if status == "Live" {
            break;
        }
        pass(&mut d).await;
        settle_tolerant(&mut d).await;
    }
    let status = status_of(&trellis, "c").await;
    let self_check = self_check_of(&trellis, "c").await;
    let diverged_at_end = diverged(&d, &scn.target_sql, &scn.oracle_sql).await;
    let outcome = Outcome {
        drain_failures,
        status_before_pass,
        self_check_before_pass,
        diverged_before_pass,
        status_after_pass,
        diverged_after_pass,
        pauses,
        trace,
        status,
        self_check,
        diverged: diverged_at_end,
    };
    trellis.shutdown().await.expect("shutdown");
    eprintln!(
        "REPORT {place:?} | {} {change:?} | before pass: {} failed drains, {}, {}, diverged {:?} \
         | after pass: {}, diverged {:?} | pauses {:?} | rounds {:?} | end: {}, {}, diverged {:?}",
        ty.label,
        outcome.drain_failures,
        outcome.status_before_pass,
        outcome.self_check_before_pass,
        outcome.diverged_before_pass,
        outcome.status_after_pass,
        outcome.diverged_after_pass,
        outcome.pauses,
        outcome.trace,
        outcome.status,
        outcome.self_check,
        outcome.diverged
    );
    outcome
}

/// One cell. A catalog-only widening is exact all the way: the target equals
/// the source before the pass (no value rounded, no two keys merged),
/// `self_check` agrees, and the pass changes nothing: the definition stays
/// live, nothing is held and no `RESUME` is needed. Any other change must
/// converge: the end state is live, `self_check` agrees and nothing differs
/// from the source, with every pause on the way carrying its reason.
async fn cell(place: Place, ty: &Ty, change: Change) {
    let Some(row) = ty.row(change) else { return };
    if !admits(place, ty.label) {
        return;
    }
    // A relabel changes the type, so both tables' `ON` columns at once:
    // there is no one-sided pair.
    if change == Change::Relabel && place == Place::JoinOnOneSided {
        return;
    }
    // Nor does a re-type to another enum: the pair would have no `=`.
    let enum_to_enum = ty.label == "enum to enum";
    if enum_to_enum && place == Place::JoinOnOneSided {
        return;
    }
    let outcome = run(place, ty, change, row).await;
    let tag = format!("{place:?}, {} {change:?}", ty.label);
    if change == Change::CatalogOnly {
        assert_eq!(
            outcome.diverged_before_pass,
            Vec::<String>::new(),
            "{tag}: the target differs from the source before the pass (a rounded value or \
             merged keys); status {}, self_check {}",
            outcome.status_before_pass,
            outcome.self_check_before_pass
        );
        assert_eq!(
            (
                outcome.drain_failures,
                outcome.status_before_pass.as_str(),
                outcome.self_check_before_pass.as_str(),
                outcome.status_after_pass.as_str(),
            ),
            (0, "Live", "Converged", "Live"),
            "{tag}: before and after the pass"
        );
        assert_eq!(
            outcome.diverged_after_pass,
            Vec::<String>::new(),
            "{tag}: after the pass"
        );
        assert!(
            outcome.pauses.is_empty(),
            "{tag}: a catalog-only widening paused the definition: {:?}",
            outcome.pauses
        );
    }
    // A passthrough copy has no modifier, so no change of one fails a write
    // or pauses it: a rewrite rebuilds it on its own (#1238, #1261).
    if place == Place::Passthrough && change != Change::Relabel && !enum_to_enum {
        assert_eq!(
            (outcome.drain_failures, outcome.pauses.as_slice()),
            (0, &[] as &[String]),
            "{tag}: a passthrough failed a write or paused"
        );
    }
    // A relabel pauses every reader of the type with its reason, whatever
    // the place (ADR-0020, #1241).
    if change == Change::Relabel {
        assert!(
            outcome
                .pauses
                .iter()
                .any(|pause| pause.contains("was renamed to 'zippy'")),
            "{tag}: the relabel paused nothing: {:?}",
            outcome.pauses
        );
    }
    // A re-type to another enum changes every value's identity: no stored
    // label is one of the new type's, so it pauses every reader of the
    // column, whatever the place, and `RESUME` rebuilds without casting
    // one (ADR-0020, #1327).
    if enum_to_enum {
        assert!(
            !outcome.pauses.is_empty(),
            "{tag}: the re-type to another enum paused nothing; trace {:?}",
            outcome.trace
        );
    }
    for pause in &outcome.pauses {
        assert!(
            pause
                .split_once(": ")
                .is_some_and(|(_, why)| !why.is_empty()),
            "{tag}: a pause without a reason: {pause}"
        );
    }
    assert_eq!(
        (outcome.status.as_str(), outcome.self_check.as_str()),
        ("Live", "Converged"),
        "{tag}: the definition did not converge; pauses {:?}",
        outcome.pauses
    );
    assert_eq!(
        outcome.diverged,
        Vec::<String>::new(),
        "{tag}: the target differs from the source at the end; pauses {:?}",
        outcome.pauses
    );
}

/// The cells [`admits`] skips are refused at define: the table is the
/// validator's, not a guess, so a type that becomes admissible in a place
/// fails here until its cells are filled in.
#[tokio::test]
async fn a_type_a_place_refuses_is_refused() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let mut accepted = Vec::new();
    for ty in TYPES {
        let Some(row) = [&ty.catalog_only, &ty.rewrite, &ty.narrow, &ty.relabel]
            .into_iter()
            .find_map(Option::as_ref)
        else {
            continue;
        };
        for place in PLACES {
            if admits(place, ty.label) {
                continue;
            }
            let scn = scenario(place, ty, row);
            let db = cluster.create_isolated_database().await;
            db.pool
                .get()
                .await
                .expect("connection")
                .batch_execute(&format!("{}{}", ty.prelude, scn.ddl))
                .await
                .expect("source DDL");
            let trellis = facade(db.dsn()).await;
            if trellis.apply(scn.definition).await.is_ok() {
                accepted.push(format!("{place:?}, {}", ty.label));
            }
            trellis.shutdown().await.expect("shutdown");
        }
    }
    assert_eq!(
        accepted,
        Vec::<String>::new(),
        "define accepts these, which `admits` skips: add their cells"
    );
}

/// A `JOIN`'s `ON` columns keep their collation in its target and hidden
/// tables by OID, whatever schema it is in: the copy names it
/// schema-qualified, so a collation of the same name earlier on Trellis's
/// `search_path` (`public` here) is never the one it gets (ADR-0020).
#[tokio::test]
async fn a_join_key_keeps_its_collation_from_another_schema() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");
    client
        .batch_execute(
            "create schema other; \
             create collation other.cc (provider = libc, locale = 'C'); \
             create collation public.cc (provider = libc, locale = 'POSIX'); \
             create table public.a (id integer primary key, k text collate other.cc, x integer); \
             create table public.b (id integer primary key, k text collate other.cc, y integer); \
             create index on public.a (k); create index on public.b (k)",
        )
        .await
        .expect("source DDL");
    let trellis = facade(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM c FROM public.a JOIN public.b ON a.k = b.k \
             GROUP BY a.k WITHOUT SOURCE INDEX SELECT a.k AS k, COUNT(*) AS n",
        )
        .await
        .expect("define the JOIN");
    let copies: Vec<(String, bool)> = client
        .query(
            &format!(
                "select c.relname::text, a.attcollation = 'other.cc'::regcollation::oid \
                 from pg_catalog.pg_attribute a \
                 join pg_catalog.pg_class c on c.oid = a.attrelid and c.relkind = 'r' \
                 join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 where a.attname = 'k' and not a.attisdropped \
                   and ((n.nspname = 'public' and c.relname = 'c') \
                        or (n.nspname = '{DEFAULT_SCHEMA}' and c.relname like 'join\\_%')) \
                 order by 1"
            ),
            &[],
        )
        .await
        .expect("the copies")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert!(
        copies.len() >= 2,
        "the target and a hidden table copy `k`: {copies:?}"
    );
    assert!(
        copies.iter().all(|(_, same)| *same),
        "a copy of `k` has another collation: {copies:?}"
    );
    trellis.shutdown().await.expect("shutdown");
}

/// ADR-0020 rule 7: a 1-1 target's copy of a `numeric(10,2)` column is an
/// unconstrained `numeric`, so a group-basis aggregate's or a `JOIN`'s `SUM`
/// over it is refused at define with the chained refusal, which names
/// #1120; the same `SUM` over the source column is accepted.
#[tokio::test]
async fn a_chained_sum_over_a_targets_numeric_copy_is_refused_by_name() {
    trellis::dev::staging::set_group_basis(true);
    let d = Driver::start(
        "create table public.t (id integer primary key, g integer, price numeric(10,2)); \
         create table public.o (id integer primary key, g integer); \
         create index on public.o (g)",
        &[("id", int4()), ("g", int4()), ("price", ValueType::Numeric)],
        &["TRANSFORM up FROM public.t SELECT g AS g, price AS p"],
        &["public.t"],
    )
    .await;
    let trellis = facade(d.db.dsn()).await;
    for chained in [
        "TRANSFORM agg FROM public.up GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(p) AS s",
        "TRANSFORM j FROM public.up JOIN public.o ON up.g = o.g GROUP BY up.g \
         WITHOUT SOURCE INDEX SELECT up.g AS g, SUM(up.p) AS s",
        "TRANSFORM javg FROM public.o JOIN public.up ON o.g = up.g GROUP BY o.g \
         WITHOUT SOURCE INDEX SELECT o.g AS g, AVG(up.p) AS s",
    ] {
        let error = trellis
            .apply(chained)
            .await
            .expect_err("a chained SUM/AVG over a target's numeric")
            .to_string();
        assert!(
            error.contains("#1120") && error.contains("a table Trellis creates"),
            "{chained}: {error}"
        );
    }
    trellis
        .apply(
            "TRANSFORM direct FROM public.t GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(price) AS s",
        )
        .await
        .expect("a SUM over the source's numeric(10,2)");
    trellis.shutdown().await.expect("shutdown");
}

/// One test per (type, change), each running the type's cells in every
/// place that admits it.
macro_rules! matrix {
    ($($name:ident: $label:literal, $change:expr;)*) => {$(
        #[tokio::test]
        async fn $name() {
            let ty = TYPES.iter().find(|t| t.label == $label).expect("a listed type");
            for place in PLACES {
                cell(place, ty, $change).await;
            }
        }
    )*};
}

matrix! {
    varchar_catalog_only: "varchar", Change::CatalogOnly;
    varchar_rewrite: "varchar", Change::Rewrite;
    varchar_narrow: "varchar", Change::Narrow;
    bpchar_rewrite: "bpchar", Change::Rewrite;
    bpchar_narrow: "bpchar", Change::Narrow;
    bpchar_to_text_rewrite: "bpchar to text", Change::Rewrite;
    bit_rewrite: "bit", Change::Rewrite;
    bit_narrow: "bit", Change::Narrow;
    varbit_catalog_only: "varbit", Change::CatalogOnly;
    varbit_rewrite: "varbit", Change::Rewrite;
    varbit_narrow: "varbit", Change::Narrow;
    numeric_catalog_only: "numeric", Change::CatalogOnly;
    numeric_rewrite: "numeric", Change::Rewrite;
    numeric_narrow: "numeric", Change::Narrow;
    time_catalog_only: "time", Change::CatalogOnly;
    time_narrow: "time", Change::Narrow;
    timetz_catalog_only: "timetz", Change::CatalogOnly;
    timetz_narrow: "timetz", Change::Narrow;
    timestamp_catalog_only: "timestamp", Change::CatalogOnly;
    timestamp_narrow: "timestamp", Change::Narrow;
    timestamptz_catalog_only: "timestamptz", Change::CatalogOnly;
    timestamptz_narrow: "timestamptz", Change::Narrow;
    interval_catalog_only: "interval", Change::CatalogOnly;
    interval_narrow: "interval", Change::Narrow;
    interval_fields_catalog_only: "interval fields", Change::CatalogOnly;
    enum_relabel: "enum", Change::Relabel;
    enum_to_enum_rewrite: "enum to enum", Change::Rewrite;
}

/// ADR-0020 rule 6, at define: a Trellis target's copy of `k` (no modifier)
/// pairs in a `JOIN`'s `ON` with a source column of the same type however
/// that column is modified, for every type the `ON` admits and every kind of
/// change to the source column.
#[tokio::test]
async fn a_trellis_copy_pairs_with_a_source_column_of_any_modifier() {
    trellis::dev::staging::set_group_basis(true);
    let mut refused = Vec::new();
    let mut tried = 0;
    // A re-type to another enum changes the type, not a modifier: the pair
    // would have no `=`.
    for ty in TYPES
        .iter()
        .filter(|ty| admits(Place::JoinOn, ty.label) && ty.label != "enum to enum")
    {
        for row in [&ty.catalog_only, &ty.rewrite, &ty.narrow]
            .into_iter()
            .filter_map(Option::as_ref)
        {
            // `b.k` as it starts, and as it is after the change.
            for altered in [false, true] {
                let d = Driver::start(
                    &format!(
                        "create table public.t (id integer primary key, k {0}, x integer); \
                         create table public.b (id integer primary key, k {0}, y integer); \
                         create index on public.b (k)",
                        row.start
                    ),
                    &[("id", int4()), ("k", ty.value_type), ("x", int4())],
                    &["TRANSFORM up FROM public.t SELECT k AS k, x AS x"],
                    &["public.t"],
                )
                .await;
                if altered {
                    d.ctl
                        .batch_execute(&format!(
                            "alter table public.b alter column k type {}",
                            alter_clause(row, "k")
                        ))
                        .await
                        .expect("alter");
                }
                let trellis = facade(d.db.dsn()).await;
                tried += 1;
                if let Err(err) = trellis
                    .apply(
                        "TRANSFORM c FROM public.up LEFT JOIN public.b ON up.k = b.k \
                         GROUP BY up.k WITHOUT SOURCE INDEX \
                         SELECT up.k AS k, COUNT(*) AS n",
                    )
                    .await
                {
                    refused.push(format!(
                        "{} {}, altered {altered}: {err}",
                        ty.label, row.start
                    ));
                }
                trellis.shutdown().await.expect("shutdown");
            }
        }
    }
    assert!(tried >= 20, "the loop ran {tried} cells");
    assert_eq!(refused, Vec::<String>::new(), "a copy didn't pair");
}

/// ADR-0020 rule 6, end to end: a `JOIN` whose `FROM` side is a Trellis
/// target pairs that target's copy of `k` (no modifier) with a captured
/// source column whose modifier was changed, for every type the `ON` admits
/// and every kind of change, and the target equals the query over the
/// captured tables as rows reach both sides.
#[tokio::test]
async fn a_trellis_copy_joins_a_modified_source_column_end_to_end() {
    trellis::dev::staging::set_group_basis(true);
    let mut cells = 0;
    for ty in TYPES
        .iter()
        .filter(|ty| admits(Place::JoinOn, ty.label) && ty.label != "enum to enum")
    {
        for row in [&ty.catalog_only, &ty.rewrite, &ty.narrow]
            .into_iter()
            .filter_map(Option::as_ref)
        {
            let cell = format!("{} {} -> {}", ty.label, row.start, row.alter);
            let mut d = Driver::start(
                &format!(
                    "{prelude}\
                     create table public.t (id integer primary key, k {start}, x integer); \
                     create table public.b (id integer primary key, k {start}, y integer); \
                     create index on public.b (k); \
                     alter table public.b alter column k type {alter}",
                    prelude = ty.prelude,
                    start = row.start,
                    alter = alter_clause(row, "k"),
                ),
                &[("id", int4()), ("k", ty.value_type), ("x", int4())],
                &["TRANSFORM up FROM public.t SELECT k AS k, x AS x"],
                &["public.t"],
            )
            .await;
            let trellis = facade(d.db.dsn()).await;
            trellis
                .apply(
                    "TRANSFORM c FROM public.up LEFT JOIN public.b ON up.k = b.k \
                     GROUP BY up.k WITHOUT SOURCE INDEX \
                     SELECT up.k AS k, COUNT(*) AS n, SUM(b.y) AS sy",
                )
                .await
                .unwrap_or_else(|e| panic!("{cell}: define: {e}"));
            trellis.shutdown().await.expect("shutdown");
            // The staging worker's capture pass: `b` had no reader before.
            let desired = trellis::defs::tables_to_capture(d.pool())
                .await
                .expect("read the tables to capture");
            trellis::capture::reconcile::reconcile(
                &mut d.ctl,
                DEFAULT_SCHEMA,
                &desired,
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .await
            .expect("capture pass");
            trellis::intake::markers::settle_registrations(d.pool()).await;
            d.settle().await;
            assert_eq!(
                d.rows("select status::text from transform_definitions order by id")
                    .await,
                ["(live)", "(live)"],
                "{cell}"
            );
            const ACTUAL: &str = "select k::text, n, sy from public.c order by 1";
            const ORACLE: &str = "select t.k::text, count(*), sum(b.y) from public.t \
                                  left join public.b on t.k = b.k group by t.k order by 1";
            for sql in [
                format!(
                    "insert into public.t values (1, '{seed}', 1), (2, null, 2); \
                     insert into public.b values (1, '{seed}', 10), (2, '{hi}', 20), \
                         (3, '{seed}', 30)",
                    seed = row.seed,
                    hi = row.hi,
                ),
                "delete from public.b where id in (1, 3)".to_string(),
                format!(
                    "insert into public.b values (4, '{seed}', 40)",
                    seed = row.seed
                ),
            ] {
                d.user().await.batch_execute(&sql).await.expect(&sql);
                d.settle().await;
                assert_eq!(d.rows(ACTUAL).await, d.rows(ORACLE).await, "{cell}: {sql}");
            }
            assert_eq!(
                d.rows("select n, sy from public.c where k is not null")
                    .await,
                ["(1,40)"],
                "{cell}: the copy matched the modified column"
            );
            cells += 1;
        }
    }
    assert!(cells >= 10, "the loop ran {cells} cells");
}

/// ADR-0020 rule 6, at define: `text` and `varchar` of any bound pair, and a
/// bound, a precision or a length never counts; another type
/// (`integer` against `bigint`) or another collation is refused.
#[tokio::test]
async fn join_on_pairs_the_string_family_and_refuses_other_types_and_collations() {
    trellis::dev::staging::set_group_basis(true);
    let cluster = TestCluster::start();
    let define = |left: &'static str, right: &'static str, collate: &'static str| {
        let cluster = &cluster;
        async move {
            let db = cluster.create_isolated_database().await;
            db.pool
                .get()
                .await
                .expect("connection")
                .batch_execute(&format!(
                    "create collation public.other (provider = libc, locale = 'POSIX'); \
                     create table public.a (id integer primary key, k {left}); \
                     create table public.b (id integer primary key, k {right} {collate}); \
                     create index on public.a (k); create index on public.b (k)"
                ))
                .await
                .expect("source DDL");
            let trellis = facade(db.dsn()).await;
            let result = trellis
                .apply(
                    "TRANSFORM c FROM public.a JOIN public.b ON a.k = b.k \
                     GROUP BY a.k WITHOUT SOURCE INDEX SELECT a.k AS k, COUNT(*) AS n",
                )
                .await
                .map(|_| ())
                .map_err(|err| err.to_string());
            trellis.shutdown().await.expect("shutdown");
            result
        }
    };
    for (left, right) in [
        ("varchar(10)", "varchar(50)"),
        ("text", "varchar(10)"),
        ("varchar", "text"),
        ("varchar(10)", "varchar"),
        ("bit varying(5)", "bit varying(8)"),
        ("timestamp(0)", "timestamp(3)"),
        ("time(0)", "time"),
    ] {
        assert_eq!(
            define(left, right, "").await,
            Ok(()),
            "{left} against {right}"
        );
    }
    for (left, right, collate) in [
        ("integer", "bigint", ""),
        ("text", "text", "collate public.other"),
        ("varchar(10)", "text", "collate public.other"),
    ] {
        let err = define(left, right, collate)
            .await
            .expect_err(&format!("{left} against {right} {collate}"));
        assert!(
            err.contains("don't match") && err.contains("a type modifier never counts"),
            "{left} against {right} {collate}: {err}"
        );
    }
}

/// ADR-0020 rule 6, at run time: a `varchar(10)` key against a `varchar(50)`
/// one and a `text` one pair without truncating either: a 12-character key
/// matches nothing against a row of its first 10 characters, and the target
/// equals the oracle.
#[tokio::test]
async fn paired_string_keys_of_different_bounds_never_truncate() {
    trellis::dev::staging::set_group_basis(true);
    let mut d = Driver::start(
        "create table public.a (id integer primary key, k varchar(50), x integer); \
         create table public.b (id integer primary key, k varchar(10), y integer); \
         create table public.e (id integer primary key, k text, y integer); \
         create index on public.a (k); create index on public.b (k); create index on public.e (k)",
        &[
            ("id", int4()),
            ("k", ValueType::Text),
            ("x", int4()),
            ("y", int4()),
        ],
        &[
            "TRANSFORM c FROM public.a LEFT JOIN public.b ON a.k = b.k \
             GROUP BY a.k WITHOUT SOURCE INDEX SELECT a.k AS k, COUNT(*) AS n, SUM(b.y) AS sy",
            "TRANSFORM d FROM public.a LEFT JOIN public.e ON a.k = e.k \
             GROUP BY a.k WITHOUT SOURCE INDEX SELECT a.k AS k, COUNT(*) AS n, SUM(e.y) AS sy",
        ],
        &["public.a", "public.b", "public.e"],
    )
    .await;
    write(
        &d,
        "insert into public.a values (1, 'abcdefghijkl', 1), (2, 'abcdefghij', 2); \
         insert into public.b values (1, 'abcdefghij', 10); \
         insert into public.e values (1, 'abcdefghij', 10), (2, 'abcdefghijkl', 20)",
    )
    .await;
    d.settle().await;
    pass(&mut d).await;
    d.settle().await;
    for (target, other) in [("c", "b"), ("d", "e")] {
        let oracle = format!(
            "select a.k::text, count(*)::bigint, sum({other}.y)::bigint \
             from public.a left join public.{other} on a.k = {other}.k group by a.k"
        );
        assert_eq!(
            diverged(
                &d,
                &format!("select k::text, n, sy from public.{target}"),
                &oracle
            )
            .await,
            Vec::<String>::new(),
            "{target}"
        );
    }
    assert_eq!(
        d.rows("select k, sy from public.c order by k").await,
        ["(abcdefghij,10)", "(abcdefghijkl,)"],
        "the 12-character key matched nothing in the 10-character column"
    );
    let trellis = facade(d.db.dsn()).await;
    assert_eq!(status_of(&trellis, "c").await, "Live");
    assert_eq!(status_of(&trellis, "d").await, "Live");
    trellis.shutdown().await.expect("shutdown");
}
