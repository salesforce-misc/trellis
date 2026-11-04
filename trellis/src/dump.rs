//! Dumping a database's Trellis definitions as SQL, so a schema-only dump
//! can carry them (`docs/embedding.md`, schema dumps).
//!
//! `pg_dump --schema-only` carries the Trellis schema's DDL, the target tables
//! and the capture triggers, but no rows. Rails and Ecto solve the same
//! problem for their own migrations by appending `INSERT`s for
//! `schema_migrations` after the dump. [`dump_definitions_sql`] is that
//! appendix for Trellis: `INSERT`s for the rows that say what is defined, and
//! for nothing else.
//!
//! Which tables hold definitions and which hold runtime state is decided here
//! and nowhere else: [`CLASSIFICATION`] names every table in the Trellis
//! schema. The integration test `dump_definitions_sql.rs` fails when a
//! migration adds a table that is not listed, so the author of the migration
//! decides, in the same change, whether it loads.
//!
//! * [`Class::Definition`] tables load with their rows.
//! * [`Class::Seed`] tables load with the one row a migration (or, for a
//!   definition's hidden table, a define) inserts into them. Their later rows
//!   are runtime state.
//! * [`Class::Runtime`] tables load empty. Their rows describe source and
//!   target rows that a schema-only dump doesn't carry, so loading them
//!   would contradict the empty tables: a ledger row saying a key
//!   contributes to a group row that doesn't exist, for one.
//!
//! A definition of some kinds owns hidden tables in the Trellis schema, named
//! by its id: an aggregate's `agg_<id>`, an aggregate `JOIN`'s `join_<id>_*`
//! and a 1-1 `JOIN`'s `join1_<id>_*`. [`CLASSIFICATION`] lists each family as
//! one entry whose name has `<id>` where the id goes ([`names_table`]). They
//! are runtime state, derived from the source rows: the schema-only dump
//! carries their DDL, so they load empty, and the build the loaded definition
//! starts fills them. The exception is a `JOIN`'s lock row table (`_l`): a
//! define inserts its one row, which is what a page or a build start takes
//! `FOR SHARE` or `FOR UPDATE`, and nothing but a define or a `RESUME` (which
//! the loaded definition never takes) inserts it again, so it is a seed, and
//! the appendix inserts the row for each lock table the dumped instance has.
//! A logical restore (`pg_dump` with data) carries their rows, and the
//! restore's pause of every definition is what covers them: a `RESUME` drops
//! and recreates each, so none of the restored rows is read.
//!
//! A loaded definition is "defined, not built": the state a define commits.
//! The worker that starts on the loaded database builds it.

use crate::app::TrellisError;
use crate::config::Config;
use crate::pool::Pool;

/// What loading a table does.
#[derive(Debug, Clone, Copy)]
pub enum Class {
    /// Its rows load. `reset` is a JSON object merged over each row first,
    /// resetting the columns that record how far a build got.
    Definition { reset: &'static str },
    /// What its migration seeds loads: these statements, which are that
    /// migration's own, with `{s}` standing for the quoted schema name and
    /// `{s_lit}` for the same inside a string literal. For an entry whose
    /// name has [`ID_PLACEHOLDER`], they are run once for each table the
    /// entry covers that the dumped instance has, with `{t}` standing for
    /// the table's quoted name.
    Seed { statements: &'static [&'static str] },
    /// Nothing loads.
    Runtime,
}

/// The placeholder in a [`CLASSIFICATION`] name for the id of the definition
/// that owns the table.
pub const ID_PLACEHOLDER: &str = "<id>";

/// Whether the [`CLASSIFICATION`] entry named `entry` covers the table
/// `table`: the same name, or, for an entry with [`ID_PLACEHOLDER`] in it, a
/// name that has a definition id (one or more decimal digits) in its place.
pub fn names_table(entry: &str, table: &str) -> bool {
    let Some((before, after)) = entry.split_once(ID_PLACEHOLDER) else {
        return entry == table;
    };
    table
        .strip_prefix(before)
        .and_then(|rest| rest.strip_suffix(after))
        .is_some_and(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
}

const NONE: &str = "{}";

/// Every table in the Trellis schema, in the order the appendix inserts the
/// definition rows (a table after the ones it references). A name with
/// [`ID_PLACEHOLDER`] stands for one table per definition that owns the
/// family ([`names_table`]).
pub const CLASSIFICATION: &[(&str, Class)] = &[
    // Definitions.
    ("refinery_schema_history", Class::Definition { reset: NONE }),
    ("trellis_instance", Class::Definition { reset: NONE }),
    ("catalog_floor", Class::Definition { reset: NONE }),
    ("schema_nodes", Class::Definition { reset: NONE }),
    ("schema_edges", Class::Definition { reset: NONE }),
    ("source_table_versions", Class::Definition { reset: NONE }),
    (
        "transform_definitions",
        Class::Definition {
            reset: r#"{"status":"waiting_to_backfill","fuse_rearmed_at":null,"build":null,"build_seg":null}"#,
        },
    ),
    ("definition_key_types", Class::Definition { reset: NONE }),
    ("definition_types", Class::Definition { reset: NONE }),
    ("definition_enum_values", Class::Definition { reset: NONE }),
    // A migration inserts one row; every later row is runtime.
    (
        "halting_stops",
        Class::Seed {
            statements: &["insert into {s}.halting_stops (id) values (true)"],
        },
    ),
    (
        "segment_pointer",
        Class::Seed {
            // The migrations seed the pointer at slot 0 and point the slot mirror
            // (a sequence, whose value a schema-only dump doesn't carry) at it.
            statements: &[
                "insert into {s}.segment_pointer (id, active_seq, ring_slot) values (true, 1, 0)",
                "select setval('{s_lit}.ring_slot_mirror', 0, true)",
            ],
        },
    ),
    (
        "segments",
        Class::Seed {
            statements: &[
                "insert into {s}.segments (seg_seq, ring_slot, state) values (1, 0, 'active')",
            ],
        },
    ),
    // Runtime state.
    ("backfill_chunks", Class::Runtime),
    ("capture_baselines", Class::Runtime),
    ("capture_failures", Class::Runtime),
    ("capture_holdups", Class::Runtime),
    ("capture_triggers", Class::Runtime),
    ("column_deaths", Class::Runtime),
    ("column_failures", Class::Runtime),
    ("column_pause_cascades", Class::Runtime),
    ("column_status", Class::Runtime),
    ("drain_cursor", Class::Runtime),
    ("drain_holdups", Class::Runtime),
    ("drainers", Class::Runtime),
    // A 1-1 `JOIN`'s pending fan-outs name keys of its `R` and `J`.
    ("join1_fan_out", Class::Runtime),
    ("key_deaths", Class::Runtime),
    ("ledger_truncate_floor", Class::Runtime),
    ("partition_correction_progress", Class::Runtime),
    ("partition_corrections", Class::Runtime),
    ("partition_sources", Class::Runtime),
    ("capture_gates", Class::Runtime),
    ("poison", Class::Runtime),
    ("poison_held", Class::Runtime),
    ("resume_requests", Class::Runtime),
    ("retype_causes", Class::Runtime),
    ("retype_releases", Class::Runtime),
    ("seg_0", Class::Runtime),
    ("seg_1", Class::Runtime),
    ("seg_2", Class::Runtime),
    ("seg_3", Class::Runtime),
    ("seg_claims", Class::Runtime),
    ("self_check_jobs", Class::Runtime),
    ("transform_fuse_gate", Class::Runtime),
    ("upstream_releases", Class::Runtime),
    ("worker_registry", Class::Runtime),
    // The hidden tables of one definition each, derived from its source rows.
    // A group-basis aggregate's partials.
    ("agg_<id>", Class::Runtime),
    // An aggregate `JOIN`'s: the `FROM` side's partials, the joined side's,
    // the join keys' match counts, the target groups' partials, and the lock
    // table, whose one row `TRUNCATE`, pages and a build start take.
    ("join_<id>_p", Class::Runtime),
    ("join_<id>_m", Class::Runtime),
    ("join_<id>_j", Class::Runtime),
    ("join_<id>_t", Class::Runtime),
    (
        "join_<id>_l",
        Class::Seed {
            statements: &["insert into {s}.{t} default values"],
        },
    ),
    // A 1-1 `JOIN`'s: the two sides' entries, the join keys' lock rows, and
    // the lock table.
    ("join1_<id>_r", Class::Runtime),
    ("join1_<id>_m", Class::Runtime),
    ("join1_<id>_j", Class::Runtime),
    (
        "join1_<id>_l",
        Class::Seed {
            statements: &["insert into {s}.{t} default values"],
        },
    ),
];

/// Statements the appendix ends with, `{s}` standing for the quoted schema
/// name: a type's OID is the database's own, so the loaded database's
/// differs from the dumped one's. Each type a definition recorded
/// (`definition_key_types`, `definition_types`, ADR-0020) is resolved again
/// by the name recorded beside it, which names the same type in the loaded
/// schema, as `RESUME` re-records it after a logical restore. A
/// `definition_types` row whose name names no type is dropped, and so is one
/// that resolves to a type the same definition records under another row.
///
/// Each enum value a definition recorded (`definition_enum_values`)
/// is resolved by its label within its type, the type named by the
/// definition's `definition_types` row for the dumped OID, before those rows
/// are replaced. One whose type or label resolves to nothing is dropped: the
/// first capture pass in the loaded database records the values of every
/// enum type a definition reads that it has none recorded for.
pub const RESOLVE_TYPE_OIDS: &[&str] = &[
    "update {s}.definition_key_types k set type_oid = t.oid \
     from pg_catalog.pg_type t join pg_catalog.pg_namespace n on n.oid = t.typnamespace \
     where n.nspname::text || '.' || t.typname::text = k.type_name",
    "create temporary table trellis_resolved_enum_values as \
     select distinct on (v.transform_id, e.oid) \
            v.transform_id, t.oid as type_oid, e.oid as value_oid, v.label \
     from {s}.definition_enum_values v \
     join {s}.definition_types r on r.transform_id = v.transform_id and r.type_oid = v.type_oid \
     join pg_catalog.pg_type t on true \
     join pg_catalog.pg_namespace n on n.oid = t.typnamespace \
     join pg_catalog.pg_enum e on e.enumtypid = t.oid and e.enumlabel::text = v.label \
     where n.nspname::text || '.' || t.typname::text = r.type_name",
    "delete from {s}.definition_enum_values",
    "create temporary table trellis_resolved_types as \
     select distinct on (r.transform_id, t.oid) r.transform_id, t.oid as type_oid, r.type_name \
     from {s}.definition_types r \
     join pg_catalog.pg_type t on true \
     join pg_catalog.pg_namespace n on n.oid = t.typnamespace \
     where n.nspname::text || '.' || t.typname::text = r.type_name",
    "delete from {s}.definition_types",
    "insert into {s}.definition_types select * from trellis_resolved_types",
    "insert into {s}.definition_enum_values select * from trellis_resolved_enum_values",
    "drop table trellis_resolved_enum_values",
    "drop table trellis_resolved_types",
];

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// `template` with each placeholder of `values` replaced by its value, in
/// one pass over the template, so a value that has a placeholder's text in
/// it (a schema named `a{t}`) is left as it is.
fn fill(template: &str, values: &[(&str, &str)]) -> String {
    let mut filled = String::with_capacity(template.len());
    let mut rest = template;
    'next: while let Some(c) = rest.chars().next() {
        for (placeholder, value) in values {
            if let Some(after) = rest.strip_prefix(placeholder) {
                filled.push_str(value);
                rest = after;
                continue 'next;
            }
        }
        filled.push(c);
        rest = &rest[c.len_utf8()..];
    }
    filled
}

/// The SQL that loads this instance's definitions into a database whose
/// Trellis schema has just been created from a schema-only dump. Append it
/// after the `pg_dump` output.
///
/// Reads one snapshot, so the rows agree with each other. Every definition
/// comes back as "defined, not built" (`waiting_to_backfill`), and the
/// instance's migration history comes back current, so `migrate` on the
/// loaded database does nothing.
pub async fn dump_definitions_sql(pool: &Pool, config: &Config) -> Result<String, TrellisError> {
    let schema = quote_ident(config.schema());
    let mut client = pool.get().await?;
    let txn = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let mut sql = String::from(
        "-- Trellis definitions (trellis::dump::dump_definitions_sql). Append to a\n\
         -- schema-only pg_dump of the Trellis schema and the schemas it maintains.\n",
    );
    // The tables of the Trellis schema in this snapshot: the hidden tables of
    // the definitions it holds are the ones a placeholder entry covers.
    let hidden_tables: Vec<String> = txn
        .query(
            "select c.relname::text from pg_catalog.pg_class c \
             where c.relnamespace = $1::text::regnamespace and c.relkind in ('r', 'p') \
             order by c.relname::text collate \"C\"",
            &[&schema],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    for (table, class) in CLASSIFICATION {
        let qualified = format!("{schema}.{}", quote_ident(table));
        match class {
            Class::Runtime => {}
            Class::Seed { statements } => {
                // A name with the id placeholder seeds each of its tables the
                // instance has, in the order of their names; any other seeds
                // its one table.
                let tables: Vec<String> = if table.contains(ID_PLACEHOLDER) {
                    hidden_tables
                        .iter()
                        .filter(|name| names_table(table, name))
                        .map(|name| quote_ident(name))
                        .collect()
                } else {
                    vec![quote_ident(table)]
                };
                let schema_lit = schema.replace('\'', "''");
                for table in tables {
                    for statement in *statements {
                        let statement = fill(
                            statement,
                            &[("{s_lit}", &schema_lit), ("{s}", &schema), ("{t}", &table)],
                        );
                        sql.push_str(&format!("{statement};\n"));
                    }
                }
            }
            Class::Definition { reset } => {
                // The table name goes in as a `%s` argument, not into the
                // template, where a `%` in the schema name would be read as
                // a format specifier. The rows are ordered by their dumped
                // text, not left in heap order, so two reads of unchanged
                // definitions give the same SQL even after a runtime update
                // (a build moving a status) has moved a row: a hook that
                // compares reads taken around its pg_dump relies on it. An
                // empty table gives no row, and no statement.
                let Some(row) = txn
                    .query_opt(
                        &format!(
                            "select format( \
                                 'insert into %s overriding system value \
                                  select * from jsonb_populate_recordset(null::%s, %L::jsonb);', \
                                 $2::text, $2::text, \
                                 jsonb_agg(to_jsonb(t) || $1::text::jsonb \
                                           order by (to_jsonb(t) || $1::text::jsonb)::text)) \
                             from {qualified} t having count(*) > 0",
                        ),
                        &[
                            reset as &(dyn tokio_postgres::types::ToSql + Sync),
                            &qualified,
                        ],
                    )
                    .await?
                else {
                    continue;
                };
                let row: String = row.get(0);
                sql.push_str(&row);
                sql.push('\n');
                // The dump creates the table's sequences at 1 and carries no
                // sequence values, so a later insert would collide with the
                // rows just loaded.
                let owned = txn
                    .query(
                        "select a.attname from pg_attribute a \
                         where a.attrelid = $1::text::regclass and a.attnum > 0 \
                           and not a.attisdropped \
                           and pg_get_serial_sequence($1, a.attname) is not null",
                        &[&qualified],
                    )
                    .await?;
                for column in owned {
                    let column: String = column.get(0);
                    let quoted = quote_ident(&column);
                    sql.push_str(&format!(
                        "select setval(pg_get_serial_sequence('{lit}', '{col_lit}'), \
                         (select max({quoted}) from {qualified}));\n",
                        lit = qualified.replace('\'', "''"),
                        col_lit = column.replace('\'', "''"),
                    ));
                }
            }
        }
    }
    for statement in RESOLVE_TYPE_OIDS {
        sql.push_str(&format!("{};\n", statement.replace("{s}", &schema)));
    }
    txn.commit().await?;
    Ok(sql)
}

/// The table a schema dump's appendix creates in the Trellis schema
/// ([`schema_dump`]): one row per definition, which `migrate` restores and
/// then drops ([ADR-0025](../../docs/decisions/0025-schema-dumps-restore-definitions.md)).
///
/// Its shape is permanent: a checked-in dump outlives the release that
/// wrote it, so every later release reads these columns as they are, and
/// anything a later release needs to say about a definition goes into its
/// floor (ADR-0024 rule 6).
///
/// ```sql
/// create table <schema>.restore_queue (
///     ordinal integer primary key,     -- the restore order, from 1
///     source_table text not null,      -- schema.table the text's FROM resolved to
///     target_table text not null,      -- schema.table of its target
///     join_table text,                 -- schema.table of a JOIN's other table
///     definition_text text not null,   -- the text, as written
///     grammar_version integer not null,-- the grammar it was written under (ADR-0018)
///     floor_version text not null      -- its floor (ADR-0024)
/// )
/// ```
pub const RESTORE_QUEUE: &str = "restore_queue";

/// The columns of [`RESTORE_QUEUE`], in order.
const QUEUE_COLUMNS: &[&str] = &[
    "ordinal",
    "source_table",
    "target_table",
    "join_table",
    "definition_text",
    "grammar_version",
    "floor_version",
];

/// What a schema dump of an embedded database needs from Trellis
/// ([`crate::Trellis::schema_dump`]): the `pg_dump` arguments that leave Trellis's own
/// tables out, and the SQL to append after the dump.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SchemaDump {
    /// Arguments for `pg_dump`, each one argument (not shell words): the
    /// Trellis schema and each 1-1 target's ledger table excluded. They
    /// add to whatever `--schema` arguments the dump has, and the target
    /// schema has to be one of those when there are any.
    pub pg_dump_args: Vec<String>,
    /// The SQL to append after `pg_dump`'s output, once the output has been
    /// through [`crate::Trellis::filter_schema_dump`]: it creates the
    /// restore queue, a `restore_queue` table in the Trellis schema, and
    /// inserts one row per definition, one row per line, in restore order.
    /// `migrate` restores them and drops the queue.
    pub appendix: String,
}

/// A name as a `pg_dump` pattern that matches exactly it: double-quoted, so
/// case and the pattern characters (`*`, `?`, `.`) are taken literally.
fn exact_pattern(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `text` as an SQL literal on one line: an escape string (`E'…'`) when it
/// has a backslash or a control character in it, so a definition written
/// over several lines is still one line of the appendix.
fn one_line_literal(text: &str) -> String {
    if !text.chars().any(|c| c == '\\' || c.is_control()) {
        return format!("'{}'", text.replace('\'', "''"));
    }
    let mut literal = String::from("E'");
    for c in text.chars() {
        match c {
            '\\' => literal.push_str("\\\\"),
            '\'' => literal.push_str("''"),
            '\n' => literal.push_str("\\n"),
            '\r' => literal.push_str("\\r"),
            '\t' => literal.push_str("\\t"),
            c if c.is_control() => literal.push_str(&format!("\\u{:04x}", c as u32)),
            c => literal.push(c),
        }
    }
    literal.push('\'');
    literal
}

/// One definition as the dump records it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Recorded {
    id: i64,
    source_table: String,
    target_table: String,
    join_table: Option<String>,
    definition_text: String,
    grammar_version: i32,
    floor_version: String,
}

/// `definitions` in restore order: each after every definition whose target
/// it reads (a chain's upstream first), and otherwise in id order, the
/// order they were defined in. A cycle can't be stored, so every
/// definition is placed; one that somehow is in a cycle goes last, in id
/// order, and its define refuses it.
fn restore_order(mut definitions: Vec<Recorded>) -> Vec<Recorded> {
    definitions.sort_by_key(|d| d.id);
    let mut placed: Vec<Recorded> = Vec::with_capacity(definitions.len());
    let mut left = definitions;
    while !left.is_empty() {
        let unplaced_targets: std::collections::HashSet<String> =
            left.iter().map(|d| d.target_table.clone()).collect();
        let next = left.iter().position(|d| {
            std::iter::once(&d.source_table)
                .chain(d.join_table.as_ref())
                .all(|read| read == &d.target_table || !unplaced_targets.contains(read))
        });
        placed.push(left.remove(next.unwrap_or(0)));
    }
    placed
}

/// The schema dump of this instance's definitions, for a dump of an
/// embedded database that leaves Trellis's internal state out
/// (`docs/embedding.md`, schema dumps): `pg_dump` with
/// [`SchemaDump::pg_dump_args`], its output through [`filter_pg_dump`],
/// then [`SchemaDump::appendix`]. `migrate` on a database loaded from it
/// restores each definition into its dumped target.
///
/// Reads one snapshot. Two reads of the same definitions give the same
/// value: no timestamps, ids or runtime state are in it, so a dump hook can
/// compare reads taken around its `pg_dump`, and a regenerated dump changes
/// only where a definition did.
pub async fn schema_dump(pool: &Pool, config: &Config) -> Result<SchemaDump, TrellisError> {
    let schema = config.schema();
    let quoted_schema = quote_ident(schema);
    let mut client = pool.get().await?;
    let txn = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let rows = txn
        .query(
            &format!(
                "select id, source_table, target_table, join_table, definition_text, \
                        grammar_version, floor_version \
                 from {quoted_schema}.transform_definitions order by id"
            ),
            &[],
        )
        .await?;
    let definitions: Vec<Recorded> = rows
        .iter()
        .map(|row| Recorded {
            id: row.get(0),
            source_table: row.get(1),
            target_table: row.get(2),
            join_table: row.get(3),
            definition_text: row.get(4),
            grammar_version: row.get(5),
            floor_version: row.get(6),
        })
        .collect();
    // Each target's ledger that exists: a 1-1 target's.
    let mut ledgers: Vec<String> = Vec::new();
    for definition in &definitions {
        let Some((target_schema, target)) = definition.target_table.split_once('.') else {
            continue;
        };
        let ledger = crate::defs::ledger::ledger_table_name(target);
        let exists: bool = txn
            .query_one(
                "select pg_catalog.to_regclass($1) is not null",
                &[&format!(
                    "{}.{}",
                    quote_ident(target_schema),
                    quote_ident(&ledger)
                )],
            )
            .await?
            .get(0);
        if exists {
            ledgers.push(format!(
                "{}.{}",
                exact_pattern(target_schema),
                exact_pattern(&ledger)
            ));
        }
    }
    txn.commit().await?;
    ledgers.sort();

    let mut pg_dump_args = vec![format!("--exclude-schema={}", exact_pattern(schema))];
    pg_dump_args.extend(ledgers.into_iter().map(|l| format!("--exclude-table={l}")));

    let queue = format!("{quoted_schema}.{RESTORE_QUEUE}");
    let mut appendix = format!(
        "-- Trellis definitions (trellis::dump::schema_dump): `migrate` restores each into its\n\
         -- target and drops the queue (docs/embedding.md, schema dumps).\n\
         create schema if not exists {quoted_schema};\n\
         create table {queue} (ordinal integer primary key, source_table text not null, \
         target_table text not null, join_table text, definition_text text not null, \
         grammar_version integer not null, floor_version text not null);\n"
    );
    for (ordinal, definition) in restore_order(definitions).into_iter().enumerate() {
        appendix.push_str(&format!(
            "insert into {queue} values ({}, {}, {}, {}, {}, {}, {});\n",
            ordinal + 1,
            one_line_literal(&definition.source_table),
            one_line_literal(&definition.target_table),
            definition
                .join_table
                .as_deref()
                .map_or_else(|| "null".to_string(), one_line_literal),
            one_line_literal(&definition.definition_text),
            definition.grammar_version,
            one_line_literal(&definition.floor_version),
        ));
    }
    Ok(SchemaDump {
        pg_dump_args,
        appendix,
    })
}

/// Both spellings `pg_dump` may write `name` in: as is, and double-quoted
/// (it quotes a name that isn't all lower case letters, digits and
/// underscores, or is a keyword).
fn spellings(name: &str) -> [String; 2] {
    [name.to_string(), quote_ident(name)]
}

/// The trigger `line` creates, as `(table, trigger)` spelled as `pg_dump`
/// writes them, when it is the `CREATE TRIGGER` of one of `triggers` running
/// a function in `schema`.
fn capture_trigger(
    line: &str,
    triggers: &[[String; 2]],
    schema: &[String; 2],
) -> Option<(String, String)> {
    let rest = line.strip_prefix("CREATE TRIGGER ")?;
    let name = triggers.iter().flatten().find(|name| {
        rest.strip_prefix(name.as_str())
            .is_some_and(|after| after.starts_with(' '))
    })?;
    if !schema
        .iter()
        .any(|s| line.contains(&format!(" EXECUTE FUNCTION {s}.")))
    {
        return None;
    }
    // `… ON <table> [REFERENCING …] FOR EACH …`: a capture trigger has no
    // other clause between its table and `FOR EACH`.
    let end = line
        .find(" REFERENCING ")
        .or_else(|| line.find(" FOR EACH "))?;
    let start = line[..end].rfind(" ON ")? + " ON ".len();
    Some((line[start..end].to_string(), name.clone()))
}

/// Whether `line` is the `ALTER TABLE … ENABLE ALWAYS TRIGGER` of one of
/// `removed`, the `(table, trigger)` pairs the filter took out.
fn is_capture_enable(line: &str, removed: &[(String, String)]) -> bool {
    removed.iter().any(|(table, trigger)| {
        line == format!("ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger};")
            || line == format!("ALTER TABLE ONLY {table} ENABLE ALWAYS TRIGGER {trigger};")
    })
}

/// `sql`, a `pg_dump --schema-only` of the application's schemas run with
/// [`SchemaDump::pg_dump_args`], without the capture triggers of the
/// Trellis instance whose schema is `schema` on its source tables.
///
/// `pg_dump` can leave out a schema or a table, but not one trigger, and a
/// source table's triggers run functions in the Trellis schema, which the
/// dump leaves out. Each trigger is two statements, each on a line of its
/// own: its `CREATE TRIGGER`, which names the function it runs, and then its
/// `ALTER TABLE … ENABLE ALWAYS TRIGGER`. Both go, with the comment block
/// `pg_dump` writes before them. Nothing else changes, `\restrict` lines
/// included.
///
/// Refuses with [`crate::error::Error::SchemaDump`] when a line still runs
/// a function in `schema` afterwards: the load would fail on it.
pub fn filter_pg_dump(sql: &str, schema: &str) -> Result<String, crate::error::Error> {
    let triggers: Vec<[String; 2]> = crate::capture::sql::CaptureEvent::ALL
        .iter()
        .map(|event| spellings(&crate::capture::sql::trigger_name(schema, *event)))
        .collect();
    let schema_spellings = spellings(schema);
    let lines: Vec<&str> = sql.split_inclusive('\n').collect();
    let bare = |i: usize| lines.get(i).map(|l| l.trim_end_matches(['\n', '\r']));
    let mut kept: Vec<&str> = Vec::with_capacity(lines.len());
    let mut removed: Vec<(String, String)> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let line = bare(i).unwrap_or_default();
        // `pg_dump`'s comment block before a trigger: `--`, a line naming
        // it, `--`, a blank line. It goes when the trigger does.
        let header_is_trigger = line == "--"
            && bare(i + 1)
                .is_some_and(|l| l.starts_with("-- Name: ") && l.contains("; Type: TRIGGER;"))
            && bare(i + 2) == Some("--")
            && bare(i + 3) == Some("");
        if header_is_trigger
            && bare(i + 4)
                .is_some_and(|l| capture_trigger(l, &triggers, &schema_spellings).is_some())
        {
            i += 4;
            continue;
        }
        let trigger = capture_trigger(line, &triggers, &schema_spellings);
        let enable = is_capture_enable(line, &removed);
        if trigger.is_some() || enable {
            removed.extend(trigger);
            i += 1;
            // The blank lines `pg_dump` writes after the statement.
            while bare(i) == Some("") {
                i += 1;
            }
            continue;
        }
        kept.push(lines[i]);
        i += 1;
    }
    let filtered = kept.concat();
    if let Some(missed) = filtered.lines().find(|line| {
        schema_spellings
            .iter()
            .any(|s| line.contains(&format!(" EXECUTE FUNCTION {s}.")))
    }) {
        return Err(crate::error::Error::SchemaDump(format!(
            "this line of pg_dump's output runs a function in the Trellis schema {schema:?}, \
             which the dump leaves out, so the dump wouldn't load: {missed}"
        )));
    }
    Ok(filtered)
}

/// Restores the definitions a schema dump queued ([`RESTORE_QUEUE`]), in
/// order, then drops the queue. `migrate` runs it after the migrations and
/// the grammar check, under the attach and handover locks (ADR-0023), so
/// it never runs beside a live process of another version. Returns how many
/// definitions it restored, the paused ones included; `0` when there is no
/// queue.
///
/// Each row is restored in its own transaction, which deletes the row: a
/// refusal leaves the rows from it on queued, and the next `migrate` goes
/// on from there.
///
/// * A row this release can't run is stored paused without being parsed
///   (ADR-0024 rule 3): its floor is above this release, its grammar
///   version is outside what this release reads, this release refuses its
///   text (ADR-0018), or a definition it reads from was stored paused.
/// * Any other row is defined at its recorded grammar version, adopting the
///   target the dump created ([`crate::defs::catalog::Restoring`]). The text
///   must resolve to the source, target and joined table the dump recorded
///   first: a different `target_schema` setting, say, would resolve a bare
///   name elsewhere, and that refuses rather than define a transform of
///   other tables.
pub(crate) async fn restore_queued(
    pool: &Pool,
    config: &Config,
) -> Result<usize, crate::error::Error> {
    use crate::error::Error;
    let queue = format!("{}.{RESTORE_QUEUE}", quote_ident(config.schema()));
    let client = pool.get().await?;
    let columns: Vec<String> = client
        .query(
            "select a.attname::text from pg_catalog.pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) and a.attnum > 0 \
               and not a.attisdropped order by a.attnum",
            &[&queue],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if columns.is_empty() {
        return Ok(0);
    }
    // ADR-0024 rule 6: a queue of another shape was written by a format
    // this release doesn't know, and isn't read as if it were this one.
    if columns != QUEUE_COLUMNS {
        return Err(Error::IncompatibleInstance(format!(
            "the schema dump's restore queue {queue} has the columns {columns:?}, and this \
             release reads {QUEUE_COLUMNS:?}: load the dump with the release that wrote it, \
             or a later one"
        )));
    }
    let rows = client
        .query(
            &format!(
                "select ordinal, source_table, target_table, join_table, definition_text, \
                        grammar_version, floor_version \
                 from {queue} order by ordinal"
            ),
            &[],
        )
        .await?;
    drop(client);
    // The targets of the rows stored paused: a row reading one is too.
    let mut paused_targets: Vec<String> = Vec::new();
    for row in &rows {
        let ordinal: i32 = row.get(0);
        let unparsed = crate::defs::catalog::Unparsed {
            source_table: row.get(1),
            target_table: row.get(2),
            join_table: row.get(3),
            definition_text: row.get(4),
            grammar_version: row.get(5),
            floor_version: row.get(6),
        };
        let restoring = crate::defs::catalog::Restoring {
            grammar_version: unparsed.grammar_version,
            queue: &queue,
            ordinal,
        };
        let refused = |code, message: String| Error::RestoreDefinition {
            target: unparsed.target_table.to_string(),
            code,
            message,
        };
        let catalog = |err: crate::defs::CatalogError| refused(err.code(), err.to_string());
        let target = unparsed.target_table;
        let upstream_paused = std::iter::once(unparsed.source_table)
            .chain(unparsed.join_table)
            .find(|read| paused_targets.iter().any(|t| t == read));
        // Why this release can't run the row, if it can't: the attach's own
        // reasons (`Floors::unrunnable`, ADR-0024 rule 3) first, then a
        // paused upstream, then text this release refuses (ADR-0018).
        let held = if let Some(unrunnable) = pool
            .floors()
            .unrunnable(unparsed.floor_version, unparsed.grammar_version)
        {
            Some(crate::defs::catalog::Held::Unrunnable(
                unrunnable.reason(target),
            ))
        } else if let Some(upstream) = upstream_paused {
            Some(crate::defs::catalog::Held::Other(format!(
                "transform {target} reads {upstream}, whose transform the restore of the schema \
                 dump stored paused; RESUME that transform, then this one"
            )))
        } else {
            match crate::defs::grammar::load_in(
                unparsed.definition_text,
                unparsed.grammar_version,
                crate::defs::grammar::OLDEST_SUPPORTED_GRAMMAR_VERSION,
                crate::defs::grammar::CURRENT_GRAMMAR_VERSION,
            ) {
                Err(err) => Some(crate::defs::catalog::Held::Other(
                    crate::defs::grammar::unparsable_stored_text_reason(target, &err),
                )),
                Ok(_) => None,
            }
        };
        if let Some(held) = held {
            crate::defs::catalog::store_unrunnable(pool, &unparsed, &held, &restoring)
                .await
                .map_err(catalog)?;
            crate::instance_log::warn!(target = %target, reason = %held.reason(), "restored paused");
            paused_targets.push(target.to_string());
            continue;
        }
        let def = crate::defs::grammar::load_in(
            unparsed.definition_text,
            unparsed.grammar_version,
            crate::defs::grammar::OLDEST_SUPPORTED_GRAMMAR_VERSION,
            crate::defs::grammar::CURRENT_GRAMMAR_VERSION,
        )
        .map_err(|err| refused(err.code(), err.to_string()))?;
        // What the text resolves to here, against what the dump recorded.
        let resolve = async |explicit: Option<&str>, bare: &str| match explicit {
            Some(schema) => crate::intake::qualify(schema, bare)
                .map_err(|err| catalog(crate::defs::CatalogError::from(err))),
            None => crate::defs::catalog::resolve_graph_identity(pool, bare)
                .await
                .map_err(catalog),
        };
        let resolved_target = crate::intake::qualify(
            crate::defs::catalog::effective_target_schema(&def, config.target_schema()),
            &def.target,
        )
        .map_err(|err| catalog(crate::defs::CatalogError::from(err)))?;
        let resolved_source = resolve(def.explicit_source_schema.as_deref(), &def.source).await?;
        let resolved_join = match crate::defs::join::spec(&def) {
            Some(join) => Some(resolve(join.explicit_right_schema.as_deref(), &join.right).await?),
            None => None,
        };
        let recorded = (
            unparsed.source_table,
            unparsed.target_table,
            unparsed.join_table,
        );
        let now = (
            resolved_source.as_str(),
            resolved_target.as_str(),
            resolved_join.as_deref(),
        );
        if recorded != now {
            return Err(refused(
                crate::ErrorCode::Validation,
                format!(
                    "its text reads {} into {}{} here, and it read {} into {}{} where it was \
                     dumped; restore with the settings it was dumped with (target_schema, the \
                     instance schema), or define it again",
                    now.0,
                    now.1,
                    now.2
                        .map(|j| format!(" joined with {j}"))
                        .unwrap_or_default(),
                    recorded.0,
                    recorded.1,
                    recorded
                        .2
                        .map(|j| format!(" joined with {j}"))
                        .unwrap_or_default(),
                ),
            ));
        }
        let client = pool.get().await?;
        let (source_columns, virtual_columns) =
            crate::defs::catalog::live_source_columns_and_virtual(&**client, unparsed.source_table)
                .await
                .map_err(catalog)?;
        drop(client);
        if crate::defs::join::spec(&def).is_none() {
            crate::defs::catalog::reject_virtual_generated_reads(
                &def,
                None,
                unparsed.source_table,
                &virtual_columns,
            )
            .map_err(catalog)?;
        }
        crate::defs::catalog::install_definition_restoring(
            pool,
            unparsed.definition_text,
            &source_columns,
            config.target_schema(),
            Some(&restoring),
        )
        .await
        .map_err(catalog)?;
    }
    let client = pool.get().await?;
    client.batch_execute(&format!("drop table {queue}")).await?;
    Ok(rows.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `pg_dump --schema-only` of a database with a 1-1 transform of
    /// `orders` and its capture triggers, plus a user's own trigger on
    /// `orders`, by `pg_dump` 15 to 18 (each against its own server), run
    /// with the dump's arguments.
    const PG_DUMPS: &[(&str, &str)] = &[
        (
            "15",
            include_str!("../tests/fixtures/pg_dump/pg_dump_15.sql"),
        ),
        (
            "16",
            include_str!("../tests/fixtures/pg_dump/pg_dump_16.sql"),
        ),
        (
            "17",
            include_str!("../tests/fixtures/pg_dump/pg_dump_17.sql"),
        ),
        (
            "18",
            include_str!("../tests/fixtures/pg_dump/pg_dump_18.sql"),
        ),
    ];

    #[test]
    fn the_filter_removes_each_capture_trigger_and_nothing_else() {
        for (version, dump) in PG_DUMPS {
            assert_eq!(
                dump.matches("CREATE TRIGGER trellis_capture_").count(),
                5,
                "pg_dump {version}'s fixture has the five capture triggers"
            );
            let filtered = filter_pg_dump(dump, "trellis").expect("filters");
            assert!(
                !filtered.contains("trellis_capture_"),
                "pg_dump {version}: {filtered}"
            );
            assert!(
                !filtered.contains("trellis."),
                "pg_dump {version}: {filtered}"
            );
            // Every other line is kept, in order: the user's trigger and the
            // `\restrict` pair included.
            let removed: Vec<&str> = dump
                .lines()
                .filter(|line| line.contains("trellis_capture_"))
                .collect();
            assert_eq!(
                removed.len(),
                15,
                "pg_dump {version}: header, create, enable"
            );
            let kept: Vec<&str> = dump
                .lines()
                .filter(|line| !line.contains("trellis_capture_"))
                .filter(|line| !line.is_empty() && *line != "--")
                .collect();
            let after: Vec<&str> = filtered
                .lines()
                .filter(|line| !line.is_empty() && *line != "--")
                .collect();
            assert_eq!(after, kept, "pg_dump {version}");
            assert!(filtered.contains("CREATE TRIGGER orders_audit "));
            assert!(filtered.starts_with("--\n-- PostgreSQL database dump\n"));
            assert_eq!(
                filtered
                    .lines()
                    .filter(|l| l.starts_with("\\restrict "))
                    .count(),
                1
            );
            assert_eq!(
                filtered
                    .lines()
                    .filter(|l| l.starts_with("\\unrestrict "))
                    .count(),
                1
            );
            // Filtering again changes nothing.
            assert_eq!(
                filter_pg_dump(&filtered, "trellis").expect("again"),
                filtered
            );
        }
    }

    #[test]
    fn the_filter_leaves_another_instances_triggers() {
        for (version, dump) in PG_DUMPS {
            assert_eq!(
                filter_pg_dump(dump, "other").expect("filters"),
                *dump,
                "pg_dump {version}: trellis's triggers aren't other's to remove"
            );
            let unchanged = dump.replace("trellis.cap_", "elsewhere.cap_");
            assert_eq!(
                filter_pg_dump(&unchanged, "trellis").expect("filters"),
                unchanged,
                "pg_dump {version}: a trigger with the name running another schema's \
                 function is not Trellis's"
            );
        }
    }

    #[test]
    fn the_filter_handles_a_quoted_schema_and_no_comments() {
        let schema = "My Trellis";
        let insert =
            crate::capture::sql::trigger_name(schema, crate::capture::sql::CaptureEvent::ALL[0]);
        let dump = format!(
            "CREATE TABLE public.t (id bigint);\n\
             CREATE TRIGGER \"{insert}\" AFTER INSERT ON public.\"T\" FOR EACH STATEMENT \
             EXECUTE FUNCTION \"My Trellis\".cap_x();\n\
             \n\
             ALTER TABLE public.\"T\" ENABLE ALWAYS TRIGGER \"{insert}\";\n\
             \n\
             CREATE INDEX t_i ON public.t USING btree (id);\n"
        );
        assert_eq!(
            filter_pg_dump(&dump, schema).expect("filters"),
            "CREATE TABLE public.t (id bigint);\n\
             CREATE INDEX t_i ON public.t USING btree (id);\n"
        );
    }

    #[test]
    fn a_line_still_running_a_trellis_function_refuses() {
        let dump = "CREATE TRIGGER mine AFTER INSERT ON public.t FOR EACH STATEMENT \
                    EXECUTE FUNCTION trellis.cap_x();\n";
        let err = filter_pg_dump(dump, "trellis").expect_err("a miss refuses");
        assert_eq!(err.code(), crate::ErrorCode::Internal);
        assert!(err.to_string().contains("CREATE TRIGGER mine"), "{err}");
    }

    #[test]
    fn a_literal_stays_on_one_line() {
        assert_eq!(one_line_literal("a 'b'"), "'a ''b'''");
        assert_eq!(
            one_line_literal("TRANSFORM t\n  FROM s\tx\\y\r'"),
            "E'TRANSFORM t\\n  FROM s\\tx\\\\y\\r'''"
        );
        assert_eq!(one_line_literal("a\u{1}b"), "E'a\\u0001b'");
        assert!(!one_line_literal("x\ny\u{85}z").contains('\n'));
    }

    #[test]
    fn a_pattern_matches_the_name_exactly() {
        assert_eq!(exact_pattern("o1__ledger"), "\"o1__ledger\"");
        assert_eq!(exact_pattern("a\"b*"), "\"a\"\"b*\"");
    }

    fn recorded(id: i64, source: &str, target: &str, join: Option<&str>) -> Recorded {
        Recorded {
            id,
            source_table: source.into(),
            target_table: target.into(),
            join_table: join.map(Into::into),
            definition_text: String::new(),
            grammar_version: 1,
            floor_version: "0.2.0".into(),
        }
    }

    #[test]
    fn a_reader_is_restored_after_its_upstream() {
        // `c` was altered to read `d`'s target, which was defined after it;
        // `e` joins `b`'s target.
        let order = restore_order(vec![
            recorded(5, "s.x", "s.e", Some("s.b")),
            recorded(1, "s.x", "s.a", None),
            recorded(2, "s.a", "s.b", None),
            recorded(3, "s.d", "s.c", None),
            recorded(4, "s.x", "s.d", None),
        ]);
        let ids: Vec<i64> = order.iter().map(|d| d.id).collect();
        assert_eq!(ids, vec![1, 2, 4, 3, 5]);
    }
}
