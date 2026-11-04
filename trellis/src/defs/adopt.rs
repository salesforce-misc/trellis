//! What a define creates for its target, and the check that lets a define
//! with `ADOPT EXISTING TABLE` build into an existing empty table instead.
//!
//! **One source of truth.** [`TargetTableDdl`] is what a define creates: the
//! target's column list and key, rendered by its key space's renderer, and a
//! 1-1 target's ledger. A plain define runs [`TargetTableDdl::create_sql`].
//! An adopting define creates the same column list as a temporary probe
//! table, reads both tables back with one catalog query
//! ([`describe`]) and compares them ([`check_shape`]), so what an adopted
//! table must look like can't drift from what a define creates: Postgres
//! itself resolves every type, collation, nullability and key the two have.
//!
//! **Refuse by default.** [`check_shape`] accepts exactly what a target
//! Trellis created can look like after changes users may make to one:
//! indexes that aren't unique, grants, comments, row security policies,
//! extended statistics, publications, views over it, storage parameters and
//! its replica identity. Everything else refuses with a [`NotAdoptable`]
//! whose [`NotAdoptable::code`] names it, and an object depending on the
//! table of a kind the check doesn't know refuses as
//! [`NotAdoptable::Unrecognized`], so a new Postgres feature refuses until
//! the check learns it.

use std::fmt;

use tokio_postgres::GenericClient;

/// What a define creates for its visible target (and a 1-1 target's
/// ledger), as each key space's renderer gives it.
#[derive(Debug, Clone)]
pub(crate) struct TargetTableDdl {
    /// The target, quoted and qualified.
    table: String,
    /// What follows `create table <table>`: the parenthesized column list
    /// and key constraint.
    body: String,
    /// A 1-1 target's ledger statements, each led by `;`.
    ledger: Option<String>,
}

impl TargetTableDdl {
    /// Splits a renderer's `create table <table> (...)` statement for
    /// `table` (quoted and qualified), with `ledger` beside it.
    ///
    /// # Panics
    ///
    /// If `create` isn't a `create table` of `table`, a renderer bug.
    pub(crate) fn new(table: &str, create: &str, ledger: Option<String>) -> Self {
        let body = create
            .strip_prefix(&format!("create table {table} "))
            .unwrap_or_else(|| panic!("not a `create table {table}` statement: {create}"));
        Self {
            table: table.to_string(),
            body: body.to_string(),
            ledger,
        }
    }

    /// The statements a plain define runs: the target, then its ledger.
    pub(crate) fn create_sql(&self) -> String {
        format!(
            "create table {} {}{}",
            self.table,
            self.body,
            self.ledger.as_deref().unwrap_or("")
        )
    }

    /// The ledger statements alone, which an adopting define runs.
    fn ledger_sql(&self) -> Option<&str> {
        self.ledger
            .as_deref()
            .map(|sql| sql.trim_start_matches([';', ' ']))
    }
}

/// The probe an adopting define creates the target's column list as.
const PROBE: &str = "pg_temp.trellis_adopt_probe";

/// A table as [`check_shape`] compares it: what [`describe`] reads from the
/// catalog.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableDescription {
    /// `pg_class.relkind`.
    pub kind: char,
    /// `pg_class.relpersistence`.
    pub persistence: char,
    /// It is a partition.
    pub is_partition: bool,
    /// It has an inheritance parent or child.
    pub inherits: bool,
    /// It is a typed table (`create table … of <type>`).
    pub typed: bool,
    /// Its table access method.
    pub access_method: Option<String>,
    /// The extension it belongs to, if any.
    pub extension: Option<String>,
    /// Its live columns, in column order.
    pub columns: Vec<ColumnDescription>,
    /// Its constraints, not-null constraints aside (nullability is read off
    /// each column).
    pub constraints: Vec<ConstraintDescription>,
    /// Its unique indexes that back no constraint, by name.
    pub unique_indexes: Vec<String>,
    /// Its triggers, internal ones (a foreign key's) aside, by name.
    pub triggers: Vec<String>,
    /// Every object that depends on it, by [`Dependent::kind`].
    pub dependents: Vec<Dependent>,
}

/// One column of a [`TableDescription`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ColumnDescription {
    pub name: String,
    /// `format_type` of its type and modifier.
    pub type_name: String,
    /// Its collation, quoted and qualified, if it has one.
    pub collation: Option<String>,
    pub not_null: bool,
    /// It has a default (a generated column has one too).
    pub has_default: bool,
    /// `pg_attribute.attgenerated`, `'\0'` for none.
    pub generated: char,
    /// `pg_attribute.attidentity`, `'\0'` for none.
    pub identity: char,
}

/// One constraint of a [`TableDescription`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConstraintDescription {
    pub name: String,
    /// `pg_constraint.contype`.
    pub kind: char,
    /// Its key columns, in key order.
    pub columns: Vec<String>,
    pub deferrable: bool,
    /// Its index is `nulls not distinct`.
    pub nulls_not_distinct: bool,
}

impl ConstraintDescription {
    /// Whether `self` keys a table as `other` does: kind, columns in order,
    /// deferrability and nulls-distinctness, whatever the names.
    fn keys_like(&self, other: &ConstraintDescription) -> bool {
        self.kind == other.kind
            && self.columns == other.columns
            && self.deferrable == other.deferrable
            && self.nulls_not_distinct == other.nulls_not_distinct
    }

    fn render(&self) -> String {
        let kind = match self.kind {
            'p' => "primary key",
            'u' if self.nulls_not_distinct => "unique nulls not distinct",
            'u' => "unique",
            _ => "constraint",
        };
        let deferrable = if self.deferrable { " deferrable" } else { "" };
        format!("{kind} ({}){deferrable}", self.columns.join(", "))
    }
}

/// An object that depends on a table, from `pg_depend`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Dependent {
    /// `index`, `toast`, `sequence`, `rule`, `view`, `relation` (another
    /// relation kind), or the catalog the object lives in (`pg_policy`).
    pub kind: String,
    /// `pg_describe_object`'s description of it.
    pub description: String,
}

/// Dependents the check accepts: what a define creates (the row type, the
/// key and its index, the TOAST table), what's compared elsewhere (defaults,
/// constraints, triggers), and what users may add to a target.
const ACCEPTED_DEPENDENTS: &[&str] = &[
    "pg_type",
    "pg_constraint",
    "pg_attrdef",
    "pg_trigger",
    "pg_policy",
    "pg_statistic_ext",
    "pg_publication_rel",
    "index",
    "toast",
    "view",
];

/// Why a define can't adopt an existing table. [`Self::code`] names the
/// property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotAdoptable {
    /// No relation has the target's name.
    Missing,
    /// The relation isn't a plain table: a view, a materialized view, a
    /// foreign table, a sequence or an index.
    NotATable { kind: String },
    /// A partitioned table.
    Partitioned,
    /// A partition of another table.
    Partition,
    /// A table in an inheritance hierarchy.
    Inheritance,
    /// A typed table (`create table … of <type>`).
    TypedTable,
    /// An `UNLOGGED` table.
    Unlogged,
    /// A member of an extension.
    ExtensionMember { extension: String },
    /// Another table access method than define would use.
    AccessMethod { expected: String, found: String },
    /// The role lacks a privilege on the table.
    Privilege { privilege: &'static str },
    /// The role isn't the table's owner or a member of it.
    NotOwner { owner: String },
    /// The table has rows.
    NotEmpty,
    /// A column define would create is missing.
    MissingColumn { column: String },
    /// A column define wouldn't create.
    ExtraColumn { column: String },
    /// The columns are in another order.
    ColumnOrder {
        expected: Vec<String>,
        found: Vec<String>,
    },
    /// A column has another type (or type modifier).
    ColumnType {
        column: String,
        expected: String,
        found: String,
    },
    /// A column has another collation.
    ColumnCollation {
        column: String,
        expected: Option<String>,
        found: Option<String>,
    },
    /// A column is `NOT NULL` where define's isn't, or the reverse.
    ColumnNullability { column: String, not_null: bool },
    /// A column has a default.
    ColumnDefault { column: String },
    /// A generated column.
    GeneratedColumn { column: String },
    /// An identity column.
    IdentityColumn { column: String },
    /// The table isn't keyed as define keys it.
    Key {
        expected: String,
        found: Option<String>,
    },
    /// A `CHECK` constraint.
    CheckConstraint { name: String },
    /// A unique or primary-key constraint besides the key.
    UniqueConstraint { name: String },
    /// An exclusion constraint.
    ExclusionConstraint { name: String },
    /// A foreign key on the table.
    ForeignKey { name: String },
    /// A unique index besides the key's.
    UniqueIndex { name: String },
    /// A trigger.
    Trigger { name: String },
    /// A rule.
    Rule { description: String },
    /// A sequence owned by one of its columns.
    OwnedSequence { description: String },
    /// Something the check doesn't recognise: a constraint kind or a
    /// dependent object it doesn't know.
    Unrecognized { description: String },
}

impl NotAdoptable {
    /// The stable name of the refused property.
    pub fn code(&self) -> &'static str {
        match self {
            NotAdoptable::Missing => "missing",
            NotAdoptable::NotATable { .. } => "not_a_table",
            NotAdoptable::Partitioned => "partitioned",
            NotAdoptable::Partition => "partition",
            NotAdoptable::Inheritance => "inheritance",
            NotAdoptable::TypedTable => "typed_table",
            NotAdoptable::Unlogged => "unlogged",
            NotAdoptable::ExtensionMember { .. } => "extension_member",
            NotAdoptable::AccessMethod { .. } => "access_method",
            NotAdoptable::Privilege { .. } => "privilege",
            NotAdoptable::NotOwner { .. } => "not_owner",
            NotAdoptable::NotEmpty => "not_empty",
            NotAdoptable::MissingColumn { .. } => "missing_column",
            NotAdoptable::ExtraColumn { .. } => "extra_column",
            NotAdoptable::ColumnOrder { .. } => "column_order",
            NotAdoptable::ColumnType { .. } => "column_type",
            NotAdoptable::ColumnCollation { .. } => "column_collation",
            NotAdoptable::ColumnNullability { .. } => "column_nullability",
            NotAdoptable::ColumnDefault { .. } => "column_default",
            NotAdoptable::GeneratedColumn { .. } => "generated_column",
            NotAdoptable::IdentityColumn { .. } => "identity_column",
            NotAdoptable::Key { .. } => "key",
            NotAdoptable::CheckConstraint { .. } => "check_constraint",
            NotAdoptable::UniqueConstraint { .. } => "unique_constraint",
            NotAdoptable::ExclusionConstraint { .. } => "exclusion_constraint",
            NotAdoptable::ForeignKey { .. } => "foreign_key",
            NotAdoptable::UniqueIndex { .. } => "unique_index",
            NotAdoptable::Trigger { .. } => "trigger",
            NotAdoptable::Rule { .. } => "rule",
            NotAdoptable::OwnedSequence { .. } => "owned_sequence",
            NotAdoptable::Unrecognized { .. } => "unrecognized",
        }
    }
}

impl fmt::Display for NotAdoptable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotAdoptable::Missing => write!(
                f,
                "no table has that name; define without ADOPT EXISTING TABLE to create it"
            ),
            NotAdoptable::NotATable { kind } => {
                write!(f, "it is a {kind}, not a plain table")
            }
            NotAdoptable::Partitioned => write!(f, "it is partitioned"),
            NotAdoptable::Partition => write!(f, "it is a partition of another table"),
            NotAdoptable::Inheritance => {
                write!(f, "it has an inheritance parent or child")
            }
            NotAdoptable::TypedTable => write!(f, "it is a typed table (OF <type>)"),
            NotAdoptable::Unlogged => write!(f, "it is UNLOGGED"),
            NotAdoptable::ExtensionMember { extension } => {
                write!(f, "it belongs to extension \"{extension}\"")
            }
            NotAdoptable::AccessMethod { expected, found } => write!(
                f,
                "it uses table access method \"{found}\", not \"{expected}\""
            ),
            NotAdoptable::Privilege { privilege } => write!(
                f,
                "the defining role lacks {privilege} on it; grant it, since Trellis writes \
                 the table as this role"
            ),
            NotAdoptable::NotOwner { owner } => write!(
                f,
                "it is owned by \"{owner}\", and the defining role is not a member of that \
                 role; Trellis alters and drops its targets, which takes ownership \
                 (ALTER TABLE … OWNER TO the defining role, or grant it membership)"
            ),
            NotAdoptable::NotEmpty => write!(
                f,
                "it has rows; Trellis adopts only an empty table (empty it, or define \
                 without ADOPT EXISTING TABLE under another name)"
            ),
            NotAdoptable::MissingColumn { column } => {
                write!(f, "it has no column \"{column}\", which define creates")
            }
            NotAdoptable::ExtraColumn { column } => {
                write!(f, "it has column \"{column}\", which define doesn't create")
            }
            NotAdoptable::ColumnOrder { expected, found } => write!(
                f,
                "its columns are in the order ({}), not ({})",
                found.join(", "),
                expected.join(", ")
            ),
            NotAdoptable::ColumnType {
                column,
                expected,
                found,
            } => write!(
                f,
                "column \"{column}\" is {found}, not {expected} (define creates every column \
                 without a type modifier)"
            ),
            NotAdoptable::ColumnCollation {
                column,
                expected,
                found,
            } => write!(
                f,
                "column \"{column}\" has collation {}, not {}",
                found.as_deref().unwrap_or("none"),
                expected.as_deref().unwrap_or("none")
            ),
            NotAdoptable::ColumnNullability { column, not_null } => {
                if *not_null {
                    write!(f, "column \"{column}\" is NOT NULL, which define's isn't")
                } else {
                    write!(f, "column \"{column}\" allows NULL, which define's doesn't")
                }
            }
            NotAdoptable::ColumnDefault { column } => {
                write!(f, "column \"{column}\" has a default")
            }
            NotAdoptable::GeneratedColumn { column } => {
                write!(f, "column \"{column}\" is generated")
            }
            NotAdoptable::IdentityColumn { column } => {
                write!(f, "column \"{column}\" is an identity column")
            }
            NotAdoptable::Key { expected, found } => match found {
                Some(found) => write!(f, "it is keyed by {found}, not {expected}"),
                None => write!(f, "it has no {expected}, the key define creates"),
            },
            NotAdoptable::CheckConstraint { name } => {
                write!(f, "it has CHECK constraint \"{name}\"")
            }
            NotAdoptable::UniqueConstraint { name } => write!(
                f,
                "it has unique constraint \"{name}\" besides the key define creates"
            ),
            NotAdoptable::ExclusionConstraint { name } => {
                write!(f, "it has exclusion constraint \"{name}\"")
            }
            NotAdoptable::ForeignKey { name } => {
                write!(f, "it has foreign key \"{name}\"")
            }
            NotAdoptable::UniqueIndex { name } => write!(
                f,
                "it has unique index \"{name}\" besides the key define creates"
            ),
            NotAdoptable::Trigger { name } => write!(f, "it has trigger \"{name}\""),
            NotAdoptable::Rule { description } => write!(f, "it has {description}"),
            NotAdoptable::OwnedSequence { description } => {
                write!(f, "{description} is owned by one of its columns")
            }
            NotAdoptable::Unrecognized { description } => write!(
                f,
                "{description} depends on it, which Trellis doesn't recognise on a target"
            ),
        }
    }
}

/// Whether `actual` has exactly the shape `expected` (the probe of what
/// define creates) has, allowing only what users may add to a target.
/// The first difference found refuses.
///
/// Column order counts: define creates the columns in the definition's
/// order, and nothing proves no statement Trellis runs depends on it.
pub fn check_shape(
    expected: &TableDescription,
    actual: &TableDescription,
) -> Result<(), NotAdoptable> {
    match actual.kind {
        'r' => {}
        'p' => return Err(NotAdoptable::Partitioned),
        other => {
            return Err(NotAdoptable::NotATable {
                kind: relkind_name(other).to_string(),
            });
        }
    }
    if actual.is_partition {
        return Err(NotAdoptable::Partition);
    }
    if actual.inherits {
        return Err(NotAdoptable::Inheritance);
    }
    if actual.typed {
        return Err(NotAdoptable::TypedTable);
    }
    if actual.persistence != 'p' {
        return Err(NotAdoptable::Unlogged);
    }
    if let Some(extension) = &actual.extension {
        return Err(NotAdoptable::ExtensionMember {
            extension: extension.clone(),
        });
    }
    if actual.access_method != expected.access_method {
        return Err(NotAdoptable::AccessMethod {
            expected: expected.access_method.clone().unwrap_or_default(),
            found: actual.access_method.clone().unwrap_or_default(),
        });
    }
    check_columns(&expected.columns, &actual.columns)?;
    check_constraints(&expected.constraints, &actual.constraints)?;
    if let Some(name) = actual.unique_indexes.first() {
        return Err(NotAdoptable::UniqueIndex { name: name.clone() });
    }
    if let Some(name) = actual.triggers.first() {
        return Err(NotAdoptable::Trigger { name: name.clone() });
    }
    for dependent in &actual.dependents {
        let description = dependent.description.clone();
        match dependent.kind.as_str() {
            kind if ACCEPTED_DEPENDENTS.contains(&kind) => {}
            "rule" => return Err(NotAdoptable::Rule { description }),
            "sequence" => return Err(NotAdoptable::OwnedSequence { description }),
            _ => return Err(NotAdoptable::Unrecognized { description }),
        }
    }
    Ok(())
}

fn check_columns(
    expected: &[ColumnDescription],
    actual: &[ColumnDescription],
) -> Result<(), NotAdoptable> {
    for column in expected {
        if !actual.iter().any(|c| c.name == column.name) {
            return Err(NotAdoptable::MissingColumn {
                column: column.name.clone(),
            });
        }
    }
    for column in actual {
        if !expected.iter().any(|c| c.name == column.name) {
            return Err(NotAdoptable::ExtraColumn {
                column: column.name.clone(),
            });
        }
    }
    let names = |columns: &[ColumnDescription]| -> Vec<String> {
        columns.iter().map(|c| c.name.clone()).collect()
    };
    if names(expected) != names(actual) {
        return Err(NotAdoptable::ColumnOrder {
            expected: names(expected),
            found: names(actual),
        });
    }
    for (want, have) in expected.iter().zip(actual) {
        let column = have.name.clone();
        if have.generated != '\0' {
            return Err(NotAdoptable::GeneratedColumn { column });
        }
        if have.identity != '\0' {
            return Err(NotAdoptable::IdentityColumn { column });
        }
        if have.type_name != want.type_name {
            return Err(NotAdoptable::ColumnType {
                column,
                expected: want.type_name.clone(),
                found: have.type_name.clone(),
            });
        }
        if have.collation != want.collation {
            return Err(NotAdoptable::ColumnCollation {
                column,
                expected: want.collation.clone(),
                found: have.collation.clone(),
            });
        }
        if have.not_null != want.not_null {
            return Err(NotAdoptable::ColumnNullability {
                column,
                not_null: have.not_null,
            });
        }
        if have.has_default {
            return Err(NotAdoptable::ColumnDefault { column });
        }
    }
    Ok(())
}

fn check_constraints(
    expected: &[ConstraintDescription],
    actual: &[ConstraintDescription],
) -> Result<(), NotAdoptable> {
    let mut rest: Vec<&ConstraintDescription> = actual.iter().collect();
    for key in expected {
        match rest.iter().position(|c| c.keys_like(key)) {
            Some(at) => {
                rest.remove(at);
            }
            None => {
                return Err(NotAdoptable::Key {
                    expected: key.render(),
                    found: actual
                        .iter()
                        .find(|c| matches!(c.kind, 'p' | 'u'))
                        .map(ConstraintDescription::render),
                });
            }
        }
    }
    if let Some(other) = rest.first() {
        let name = other.name.clone();
        return Err(match other.kind {
            'c' => NotAdoptable::CheckConstraint { name },
            'p' | 'u' => NotAdoptable::UniqueConstraint { name },
            'x' => NotAdoptable::ExclusionConstraint { name },
            'f' => NotAdoptable::ForeignKey { name },
            't' => NotAdoptable::Trigger { name },
            kind => NotAdoptable::Unrecognized {
                description: format!("constraint \"{name}\" of kind '{kind}'"),
            },
        });
    }
    Ok(())
}

fn relkind_name(kind: char) -> &'static str {
    match kind {
        'v' => "view",
        'm' => "materialized view",
        'f' => "foreign table",
        'S' => "sequence",
        'i' | 'I' => "index",
        'c' => "composite type",
        't' => "TOAST table",
        _ => "relation of another kind",
    }
}

/// Reads table `oid` into a [`TableDescription`].
pub(crate) async fn describe(
    client: &impl GenericClient,
    oid: u32,
) -> Result<TableDescription, tokio_postgres::Error> {
    let row = client
        .query_one(
            "select c.relkind::text, c.relpersistence::text, c.relispartition, \
                    c.reloftype <> 0, am.amname::text, \
                    exists (select from pg_catalog.pg_inherits i \
                            where i.inhrelid = c.oid or i.inhparent = c.oid), \
                    (select e.extname::text from pg_catalog.pg_depend d \
                     join pg_catalog.pg_extension e on e.oid = d.refobjid \
                     where d.classid = 'pg_catalog.pg_class'::regclass and d.objid = c.oid \
                       and d.refclassid = 'pg_catalog.pg_extension'::regclass \
                       and d.deptype = 'e' limit 1) \
             from pg_catalog.pg_class c \
             left join pg_catalog.pg_am am on am.oid = c.relam \
             where c.oid = $1",
            &[&oid],
        )
        .await?;
    let first = |text: String| text.chars().next().unwrap_or('\0');
    let columns = client
        .query(
            "select a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod), \
                    case when a.attcollation = 0 then null \
                         else pg_catalog.quote_ident(cn.nspname) || '.' \
                              || pg_catalog.quote_ident(co.collname) end, \
                    a.attnotnull, a.atthasdef, a.attgenerated::text, a.attidentity::text \
             from pg_catalog.pg_attribute a \
             left join pg_catalog.pg_collation co on co.oid = a.attcollation \
             left join pg_catalog.pg_namespace cn on cn.oid = co.collnamespace \
             where a.attrelid = $1 and a.attnum > 0 and not a.attisdropped \
             order by a.attnum",
            &[&oid],
        )
        .await?
        .into_iter()
        .map(|row| ColumnDescription {
            name: row.get(0),
            type_name: row.get(1),
            collation: row.get(2),
            not_null: row.get(3),
            has_default: row.get(4),
            generated: first(row.get(5)),
            identity: first(row.get(6)),
        })
        .collect();
    let constraints = client
        .query(
            "select co.conname::text, co.contype::text, \
                    array(select a.attname::text \
                          from unnest(co.conkey) with ordinality k(attnum, ord) \
                          join pg_catalog.pg_attribute a \
                            on a.attrelid = co.conrelid and a.attnum = k.attnum \
                          order by k.ord), \
                    co.condeferrable, coalesce(i.indnullsnotdistinct, false) \
             from pg_catalog.pg_constraint co \
             left join pg_catalog.pg_index i on i.indexrelid = co.conindid \
             where co.conrelid = $1 and co.contype <> 'n' \
             order by co.conname",
            &[&oid],
        )
        .await?
        .into_iter()
        .map(|row| ConstraintDescription {
            name: row.get(0),
            kind: first(row.get(1)),
            columns: row.get(2),
            deferrable: row.get(3),
            nulls_not_distinct: row.get(4),
        })
        .collect();
    let unique_indexes = client
        .query(
            "select ic.relname::text from pg_catalog.pg_index i \
             join pg_catalog.pg_class ic on ic.oid = i.indexrelid \
             where i.indrelid = $1 and i.indisunique \
               and not exists (select from pg_catalog.pg_constraint co \
                               where co.conrelid = $1 and co.conindid = i.indexrelid) \
             order by ic.relname",
            &[&oid],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let triggers = client
        .query(
            "select tgname::text from pg_catalog.pg_trigger \
             where tgrelid = $1 and not tgisinternal order by tgname",
            &[&oid],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let dependents = client
        .query(
            "select case \
                      when d.classid = 'pg_catalog.pg_rewrite'::regclass then \
                        (select case when r.ev_class = $1 then 'rule' else 'view' end \
                         from pg_catalog.pg_rewrite r where r.oid = d.objid) \
                      when d.classid = 'pg_catalog.pg_class'::regclass then \
                        (select case dc.relkind when 'i' then 'index' when 't' then 'toast' \
                                when 'S' then 'sequence' else 'relation' end \
                         from pg_catalog.pg_class dc where dc.oid = d.objid) \
                      else (select cc.relname::text from pg_catalog.pg_class cc \
                            where cc.oid = d.classid) \
                    end, \
                    pg_catalog.pg_describe_object(d.classid, d.objid, d.objsubid) \
             from pg_catalog.pg_depend d \
             where d.refclassid = 'pg_catalog.pg_class'::regclass and d.refobjid = $1 \
             order by 2",
            &[&oid],
        )
        .await?
        .into_iter()
        .map(|row| Dependent {
            kind: row.get::<_, Option<String>>(0).unwrap_or_default(),
            description: row.get(1),
        })
        .collect();
    Ok(TableDescription {
        kind: first(row.get(0)),
        persistence: first(row.get(1)),
        is_partition: row.get(2),
        typed: row.get(3),
        access_method: row.get(4),
        inherits: row.get(5),
        extension: row.get(6),
        columns,
        constraints,
        unique_indexes,
        triggers,
        dependents,
    })
}

/// The privileges Trellis needs on a target: it reads and writes the rows
/// as the defining role, and empties it on a source's `TRUNCATE`.
const PRIVILEGES: &[&str] = &["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"];

/// Adopts `schema.table` (`qualified` quoted, as `ddl` names it) inside the
/// define's transaction `txn`: refuses it unless it is a plain table the
/// defining role can write and owns (or is a member of the owner of), empty
/// under a lock that keeps writers out until the define commits, and shaped
/// exactly as `ddl` would create it ([`check_shape`]). Then creates what
/// `ddl` creates beside it (a 1-1 target's ledger). The ledger's name
/// being taken is refused by the caller, as for a plain define.
pub(crate) async fn adopt_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    table: &str,
    ddl: &TargetTableDdl,
) -> Result<(), AdoptError> {
    let found = txn
        .query_opt(
            "select c.oid, c.relkind::text, pg_catalog.pg_get_userbyid(c.relowner)::text, \
                    pg_catalog.pg_has_role(current_user, c.relowner, 'MEMBER') \
             from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname = $2",
            &[&schema, &table],
        )
        .await?;
    let Some(found) = found else {
        return Err(NotAdoptable::Missing.into());
    };
    let oid: u32 = found.get(0);
    let kind: String = found.get(1);
    if !matches!(kind.as_str(), "r" | "p") {
        return Err(NotAdoptable::NotATable {
            kind: relkind_name(kind.chars().next().unwrap_or('\0')).to_string(),
        }
        .into());
    }
    if !found.get::<_, bool>(3) {
        return Err(NotAdoptable::NotOwner {
            owner: found.get(2),
        }
        .into());
    }
    for privilege in PRIVILEGES {
        let held: bool = txn
            .query_one(
                "select pg_catalog.has_table_privilege($1::oid, $2)",
                &[&oid, privilege],
            )
            .await?
            .get(0);
        if !held {
            return Err(NotAdoptable::Privilege { privilege }.into());
        }
    }
    // Keeps writers and DDL out until the define commits, so the table
    // checked is the table adopted and stays empty.
    txn.batch_execute(&format!(
        "lock table {} in share row exclusive mode",
        ddl.table
    ))
    .await?;
    let actual = describe(txn, oid).await?;
    if txn
        .query_one(&format!("select exists (select from {})", ddl.table), &[])
        .await?
        .get::<_, bool>(0)
    {
        return Err(NotAdoptable::NotEmpty.into());
    }
    txn.batch_execute(&format!("create temp table {PROBE} {}", ddl.body))
        .await?;
    let probe_oid: u32 = txn
        .query_one(&format!("select '{PROBE}'::regclass::oid"), &[])
        .await?
        .get(0);
    let expected = describe(txn, probe_oid).await?;
    txn.batch_execute(&format!("drop table {PROBE}")).await?;
    check_shape(&expected, &actual)?;
    if let Some(ledger) = ddl.ledger_sql() {
        txn.batch_execute(ledger).await?;
    }
    Ok(())
}

/// [`adopt_in_txn`]'s failure: a refusal, or the database's.
#[derive(Debug)]
pub(crate) enum AdoptError {
    Refused(NotAdoptable),
    Db(tokio_postgres::Error),
}

impl From<NotAdoptable> for AdoptError {
    fn from(refusal: NotAdoptable) -> Self {
        AdoptError::Refused(refusal)
    }
}

impl From<tokio_postgres::Error> for AdoptError {
    fn from(err: tokio_postgres::Error) -> Self {
        AdoptError::Db(err)
    }
}

/// `table` quoted and qualified, as a renderer names it.
#[cfg(test)]
fn quoted(schema: &str, table: &str) -> String {
    use crate::pool::quote_ident;
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, type_name: &str) -> ColumnDescription {
        ColumnDescription {
            name: name.to_string(),
            type_name: type_name.to_string(),
            ..Default::default()
        }
    }

    fn key(kind: char, columns: &[&str]) -> ConstraintDescription {
        ConstraintDescription {
            name: "t_pkey".to_string(),
            kind,
            columns: columns.iter().map(|c| c.to_string()).collect(),
            deferrable: false,
            nulls_not_distinct: kind == 'u',
        }
    }

    fn dependent(kind: &str) -> Dependent {
        Dependent {
            kind: kind.to_string(),
            description: format!("a {kind}"),
        }
    }

    /// What define would create for a 1-1 target keyed by `id`.
    fn expected() -> TableDescription {
        let mut id = column("id", "bigint");
        id.not_null = true;
        let mut region = column("region", "text");
        region.collation = Some("pg_catalog.\"default\"".to_string());
        TableDescription {
            kind: 'r',
            persistence: 't',
            access_method: Some("heap".to_string()),
            columns: vec![id, region],
            constraints: vec![key('p', &["id"])],
            dependents: vec![dependent("pg_type"), dependent("index")],
            ..Default::default()
        }
    }

    fn actual() -> TableDescription {
        let mut table = expected();
        table.persistence = 'p';
        table.constraints[0].name = "my_key".to_string();
        table
    }

    fn refusal(change: impl FnOnce(&mut TableDescription)) -> &'static str {
        let mut table = actual();
        change(&mut table);
        check_shape(&expected(), &table)
            .expect_err("refused")
            .code()
    }

    #[test]
    fn the_shape_define_creates_is_accepted_whatever_the_key_is_named() {
        assert_eq!(check_shape(&expected(), &actual()), Ok(()));
    }

    #[test]
    fn what_users_may_add_to_a_target_is_accepted() {
        let mut table = actual();
        for kind in [
            "index",
            "toast",
            "view",
            "pg_policy",
            "pg_statistic_ext",
            "pg_publication_rel",
            "pg_attrdef",
            "pg_constraint",
            "pg_trigger",
        ] {
            table.dependents.push(dependent(kind));
        }
        assert_eq!(check_shape(&expected(), &table), Ok(()));
    }

    #[test]
    fn an_aggregate_key_is_matched_with_its_nulls_not_distinct() {
        let mut want = expected();
        want.constraints = vec![key('u', &["id"])];
        let mut have = actual();
        have.constraints = vec![key('u', &["id"])];
        assert_eq!(check_shape(&want, &have), Ok(()));
        have.constraints[0].nulls_not_distinct = false;
        assert_eq!(check_shape(&want, &have).unwrap_err().code(), "key");
        have.constraints = vec![key('p', &["id"])];
        assert_eq!(check_shape(&want, &have).unwrap_err().code(), "key");
    }

    type Change = Box<dyn FnOnce(&mut TableDescription)>;

    #[test]
    fn every_other_property_refuses_with_its_code() {
        let cases: Vec<(&str, Change)> = vec![
            ("not_a_table", Box::new(|t| t.kind = 'v')),
            ("not_a_table", Box::new(|t| t.kind = 'm')),
            ("not_a_table", Box::new(|t| t.kind = 'f')),
            ("partitioned", Box::new(|t| t.kind = 'p')),
            ("partition", Box::new(|t| t.is_partition = true)),
            ("inheritance", Box::new(|t| t.inherits = true)),
            ("typed_table", Box::new(|t| t.typed = true)),
            ("unlogged", Box::new(|t| t.persistence = 'u')),
            (
                "extension_member",
                Box::new(|t| t.extension = Some("ext".to_string())),
            ),
            (
                "access_method",
                Box::new(|t| t.access_method = Some("columnar".to_string())),
            ),
            ("missing_column", Box::new(|t| drop(t.columns.pop()))),
            (
                "extra_column",
                Box::new(|t| t.columns.push(column("note", "text"))),
            ),
            ("column_order", Box::new(|t| t.columns.reverse())),
            (
                "column_type",
                Box::new(|t| t.columns[0].type_name = "integer".to_string()),
            ),
            (
                "column_type",
                Box::new(|t| t.columns[1].type_name = "character varying(10)".to_string()),
            ),
            (
                "column_collation",
                Box::new(|t| t.columns[1].collation = Some("pg_catalog.\"C\"".to_string())),
            ),
            (
                "column_nullability",
                Box::new(|t| t.columns[1].not_null = true),
            ),
            (
                "column_nullability",
                Box::new(|t| t.columns[0].not_null = false),
            ),
            (
                "column_default",
                Box::new(|t| t.columns[1].has_default = true),
            ),
            (
                "generated_column",
                Box::new(|t| {
                    t.columns[1].has_default = true;
                    t.columns[1].generated = 's';
                }),
            ),
            (
                "generated_column",
                Box::new(|t| t.columns[1].generated = 'v'),
            ),
            ("identity_column", Box::new(|t| t.columns[0].identity = 'a')),
            ("key", Box::new(|t| t.constraints.clear())),
            (
                "key",
                Box::new(|t| t.constraints = vec![key('p', &["id", "region"])]),
            ),
            ("key", Box::new(|t| t.constraints[0].deferrable = true)),
            (
                "check_constraint",
                Box::new(|t| t.constraints.push(other('c'))),
            ),
            (
                "unique_constraint",
                Box::new(|t| t.constraints.push(other('u'))),
            ),
            (
                "exclusion_constraint",
                Box::new(|t| t.constraints.push(other('x'))),
            ),
            ("foreign_key", Box::new(|t| t.constraints.push(other('f')))),
            ("trigger", Box::new(|t| t.constraints.push(other('t')))),
            ("unrecognized", Box::new(|t| t.constraints.push(other('z')))),
            (
                "unique_index",
                Box::new(|t| t.unique_indexes.push("u".to_string())),
            ),
            ("trigger", Box::new(|t| t.triggers.push("tg".to_string()))),
            ("rule", Box::new(|t| t.dependents.push(dependent("rule")))),
            (
                "owned_sequence",
                Box::new(|t| t.dependents.push(dependent("sequence"))),
            ),
            (
                "unrecognized",
                Box::new(|t| t.dependents.push(dependent("relation"))),
            ),
            (
                "unrecognized",
                Box::new(|t| t.dependents.push(dependent("pg_something_new"))),
            ),
        ];
        for (code, change) in cases {
            assert_eq!(refusal(change), code);
        }
    }

    fn other(kind: char) -> ConstraintDescription {
        ConstraintDescription {
            name: "other".to_string(),
            kind,
            columns: vec!["region".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn the_ddl_splits_into_the_body_a_probe_reuses_and_the_ledger() {
        let table = quoted("public", "t");
        let ddl = TargetTableDdl::new(
            &table,
            "create table \"public\".\"t\" (\"id\" bigint, primary key (\"id\"))",
            Some("; create table \"public\".\"t__ledger\" (k text)".to_string()),
        );
        assert_eq!(
            ddl.create_sql(),
            "create table \"public\".\"t\" (\"id\" bigint, primary key (\"id\")); \
             create table \"public\".\"t__ledger\" (k text)"
        );
        assert_eq!(
            ddl.ledger_sql(),
            Some("create table \"public\".\"t__ledger\" (k text)")
        );
    }
}
