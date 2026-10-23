//! Dumping a database's Trellis definitions as SQL, so a schema-only dump
//! can carry them (issue #644, `docs/embedding.md`, schema dumps).
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
//! * [`Class::Seed`] tables load with the one row a migration inserts into
//!   them. Their later rows are runtime state.
//! * [`Class::Runtime`] tables load empty. Their rows describe source and
//!   target rows that a schema-only dump doesn't carry, so loading them
//!   would contradict the empty tables: a ledger row saying a key
//!   contributes to a group row that doesn't exist, for one.
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
    /// `{s_lit}` for the same inside a string literal.
    Seed { statements: &'static [&'static str] },
    /// Nothing loads.
    Runtime,
}

const NONE: &str = "{}";

/// Every table in the Trellis schema, in the order the appendix inserts the
/// definition rows (a table after the ones it references).
///
/// A table created at run time under `_trellis_rel_projection_` (a
/// relationship's parent projection) is runtime state, so it is not listed;
/// see [`is_projection_table`].
pub const CLASSIFICATION: &[(&str, Class)] = &[
    // Definitions.
    ("refinery_schema_history", Class::Definition { reset: NONE }),
    ("trellis_instance", Class::Definition { reset: NONE }),
    ("schema_nodes", Class::Definition { reset: NONE }),
    ("schema_edges", Class::Definition { reset: NONE }),
    ("source_table_versions", Class::Definition { reset: NONE }),
    (
        "transform_definitions",
        Class::Definition {
            reset: r#"{"status":"waiting_to_backfill","fuse_rearmed_at":null,"build":null,"build_seg":null,"build_marker_generation":null}"#,
        },
    ),
    ("definition_key_types", Class::Definition { reset: NONE }),
    ("definition_types", Class::Definition { reset: NONE }),
    ("definition_enum_values", Class::Definition { reset: NONE }),
    (
        "relationship_definitions",
        Class::Definition { reset: NONE },
    ),
    (
        "relationship_projections",
        Class::Definition {
            reset: r#"{"refreshed_lsn":null}"#,
        },
    ),
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
            // V3 seeds the pointer at slot 0 and V53 points the slot mirror
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
    ("engine_bootstrap", Class::Runtime),
    ("key_deaths", Class::Runtime),
    ("ledger_truncate_floor", Class::Runtime),
    ("partition_correction_progress", Class::Runtime),
    ("partition_corrections", Class::Runtime),
    ("partition_sources", Class::Runtime),
    ("pending_backfill", Class::Runtime),
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
    ("worker_registry", Class::Runtime),
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
/// Each enum value a definition recorded (`definition_enum_values`, #1241)
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

/// Whether `table` is a relationship's parent projection, which Trellis
/// creates per relationship and fills from the to-side table.
#[cfg_attr(not(feature = "internals"), allow(dead_code))]
pub fn is_projection_table(table: &str) -> bool {
    table.starts_with("_trellis_rel_projection_")
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
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
    for (table, class) in CLASSIFICATION {
        let qualified = format!("{schema}.{}", quote_ident(table));
        match class {
            Class::Runtime => {}
            Class::Seed { statements } => {
                for statement in *statements {
                    let statement = statement
                        .replace("{s_lit}", &schema.replace('\'', "''"))
                        .replace("{s}", &schema);
                    sql.push_str(&format!("{statement};\n"));
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
