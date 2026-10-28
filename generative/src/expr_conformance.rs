//! The expression conformance properties: random typed expressions
//! ([`crate::expr_gen`]) checked against a real Postgres.
//!
//! After D2 Postgres computes every value, so the risk in an operator is in
//! what Rust still does: type inference, the parser's precedence and
//! associativity, rendering the tree back to SQL, and the refusals. Four
//! properties pin those, each against a throwaway table of random rows
//! (`NULL`, extremes, `NaN`, `±Infinity`, infinite dates):
//!
//! 1. **Type.** The column define creates for an admitted expression has the
//!    `format_type` of `CREATE TABLE AS SELECT <the user's text>`.
//! 2. **Value.** `SELECT <the engine's rendered SQL>` and `SELECT <the
//!    user's text>` agree on every row: equal text, or the same SQLSTATE
//!    class.
//! 3. **Round trip.** `parse(render(parse(text))) == parse(text)`, and
//!    parsing the text gives the tree it was written from.
//! 4. **Refusal.** A near miss is refused at define, with an error that
//!    names the construct.
//!
//! Postgres folds constants at plan time, so an expression it rejects for
//! every row (`2147483647 + 1`) is refused at define by design. The checker
//! treats "refused at define and Postgres raises class 22 on a where-false
//! probe" as agreement, and flags both a refusal where Postgres accepts (a
//! false refusal) and an acceptance where Postgres raises at plan time (a
//! missed refusal).

use std::collections::HashMap;

use testkit::TestDatabase;
use tokio_postgres::{Client, NoTls};
use trellis::dev::config::DEFAULT_SCHEMA;
use trellis::dev::defs::ast::{Expr, Operator, ValueType};
use trellis::dev::defs::oracle::render_expr_sql;
use trellis::dev::defs::{FloatWidth, IntWidth};
use trellis::dev::defs::{
    PgType, ValidationError, create_target_table, parse, render_definition_text,
    render_qualified_expr_sql, source_primary_key, validate,
};
use trellis::{Config, Pool};

use crate::expr_gen::{Column, NearMiss};

/// The throwaway table every expression is run against.
pub const TABLE: &str = "x";

struct Base {
    stem: &'static str,
    pg: &'static str,
    ty: ValueType,
    pool: &'static [&'static str],
}

fn bases() -> Vec<Base> {
    vec![
        Base {
            stem: "i2",
            pg: "smallint",
            ty: ValueType::Integer(IntWidth::Int2),
            pool: &["-32768", "32767", "0", "1", "-1", "100"],
        },
        Base {
            stem: "i4",
            pg: "integer",
            ty: ValueType::Integer(IntWidth::Int4),
            pool: &["-2147483648", "2147483647", "0", "1", "-1", "1000"],
        },
        Base {
            stem: "i8",
            pg: "bigint",
            ty: ValueType::Integer(IntWidth::Int8),
            pool: &[
                "-9223372036854775808",
                "9223372036854775807",
                "0",
                "1",
                "-1",
                "5000000000",
            ],
        },
        Base {
            stem: "n",
            pg: "numeric",
            ty: ValueType::Numeric,
            pool: &[
                "0",
                "1",
                "-1",
                "1.5",
                "0.000001",
                "12345678901234567890.123456789",
                "-99999999999999999999",
                "NaN",
                "100.00",
            ],
        },
        Base {
            stem: "f4",
            pg: "real",
            ty: ValueType::Float(FloatWidth::Float4),
            pool: &[
                "0",
                "-0",
                "1.5",
                "-2.5",
                "NaN",
                "Infinity",
                "-Infinity",
                "3.4028235e+38",
                "1e-45",
                "16777217",
            ],
        },
        Base {
            stem: "f8",
            pg: "double precision",
            ty: ValueType::Float(FloatWidth::Float8),
            pool: &[
                "0",
                "-0",
                "1.5",
                "NaN",
                "Infinity",
                "-Infinity",
                "1.7976931348623157e+308",
                "5e-324",
                "9007199254740993",
            ],
        },
        Base {
            stem: "t",
            pg: "text",
            ty: ValueType::Text,
            pool: &[
                "",
                "a",
                "abc",
                "aXbXc",
                "it's",
                "é",
                "日本語",
                "a b",
                "ab",
                "zzz",
                "  ",
            ],
        },
        Base {
            stem: "b",
            pg: "boolean",
            ty: ValueType::Boolean,
            pool: &["true", "false"],
        },
        Base {
            stem: "d",
            pg: "date",
            ty: ValueType::Other(PgType::Date),
            pool: &[
                "2024-02-29",
                "1970-01-01",
                "infinity",
                "-infinity",
                "4714-11-24 BC",
                "5874897-12-31",
            ],
        },
        Base {
            stem: "u",
            pg: "uuid",
            ty: ValueType::Uuid,
            pool: &[
                "00000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12",
            ],
        },
        Base {
            stem: "tm",
            pg: "time",
            ty: ValueType::Other(PgType::Time),
            pool: &["00:00:00", "23:59:59.999999", "12:30:00", "24:00:00"],
        },
        Base {
            stem: "tt",
            pg: "timetz",
            ty: ValueType::Other(PgType::TimeTz),
            pool: &["00:00:00+00", "12:30:00-05", "12:30:00+05", "23:59:59.5+14"],
        },
        Base {
            stem: "tz",
            pg: "timestamptz",
            ty: ValueType::Other(PgType::TimestampTz),
            pool: &[
                "2024-02-29 12:00:00+00",
                "2024-02-29 07:00:00-05",
                "infinity",
                "-infinity",
                "4713-01-01 00:00:00+00 BC",
            ],
        },
        Base {
            stem: "ts",
            pg: "timestamp",
            ty: ValueType::Other(PgType::Timestamp),
            pool: &[
                "2024-02-29 12:00:00",
                "1970-01-01 00:00:00.5",
                "infinity",
                "-infinity",
                "4713-01-01 00:00:00 BC",
            ],
        },
    ]
}

/// The table's data columns (two per type, `<stem>a` and `<stem>b`) and the
/// type define sees for each.
pub fn columns() -> Vec<Column> {
    bases()
        .into_iter()
        .flat_map(|b| {
            ["a", "b"]
                .into_iter()
                .map(move |suffix| (format!("{}{suffix}", b.stem), b.ty))
        })
        .collect()
}

/// Which property a divergence broke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Property {
    Type,
    Value,
    RoundTrip,
    Refusal,
    /// Define refused an expression Postgres accepts, by a refusal that is
    /// not a design refusal.
    FalseRefusal,
    /// Define admitted an expression Postgres raises on at plan time.
    MissedRefusal,
}

/// A disagreement between the engine and Postgres.
#[derive(Debug, Clone)]
pub struct Divergence {
    pub property: Property,
    pub text: String,
    pub detail: String,
}

impl std::fmt::Display for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} divergence for `{}`: {}",
            self.property, self.text, self.detail
        )
    }
}

/// What a checked expression turned out to be, when nothing diverged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Agreement {
    /// Admitted; type and values agree.
    Admitted,
    /// Refused at define (the message), and Postgres raises: on the probe, or
    /// on a `REGEXP_COUNT` pattern literal define asks it to judge.
    RefusedAndRaises(String),
    /// Admitted, but Postgres raises while planning it, and the expression
    /// holds a call whose arguments are all constants: Postgres folds it and
    /// then the sum around it, and define has no value for the call to fold
    /// (see [`KNOWN_GAP_CONSTANT_CALL`]).
    KnownGap(&'static str),
}

/// SQLSTATE `invalid_regular_expression`.
const INVALID_REGEX: &str = "2201B";

/// The open gap the property tolerates, so a fix has one place to remove.
/// Filed as #1446.
pub const KNOWN_GAP_CONSTANT_CALL: &str = "constant function calls are folded by Postgres at plan \
     time, so a range overflow around one is not refused at define";

/// Whether `expr` holds a function call (not `COALESCE`) with no column in
/// it, which Postgres evaluates while planning.
pub fn has_constant_call(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { lhs, rhs, .. } => has_constant_call(lhs) || has_constant_call(rhs),
        Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => has_constant_call(operand),
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            expr.logic_children().into_iter().any(has_constant_call)
        }
        Expr::FunctionCall { name, args } => {
            (name != "COALESCE" && name != "NULLIF" && !reads_column(expr))
                || args.iter().any(has_constant_call)
        }
        _ => false,
    }
}

/// Whether `expr` reads a column anywhere in it.
fn reads_column(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::SourceColumn { .. } => true,
        Expr::BinaryOp { lhs, rhs, .. } => reads_column(lhs) || reads_column(rhs),
        Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => reads_column(operand),
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            expr.logic_children().into_iter().any(reads_column)
        }
        Expr::FunctionCall { args, .. } => args.iter().any(reads_column),
        _ => false,
    }
}

/// The SQLSTATE of a failed query.
fn code_of(err: &tokio_postgres::Error) -> String {
    err.code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| format!("no SQLSTATE: {err}"))
}

fn class_of(code: &str) -> &str {
    code.get(..2).unwrap_or(code)
}

fn is_range_refusal(err: &ValidationError) -> bool {
    matches!(
        err,
        ValidationError::CoalesceLiteralOutOfRange { .. }
            | ValidationError::ConstantOutOfRange { .. }
            | ValidationError::FloatComparisonLiteralOutOfRange { .. }
            | ValidationError::CaseLiteralOutOfRange { .. }
    )
}

/// The table, the connection and the engine pool the properties run on.
pub struct Fixture {
    client: Client,
    pool: Pool,
    source_columns: HashMap<String, ValueType>,
    next_target: std::cell::Cell<u32>,
}

impl Fixture {
    /// Creates the table in `db` and fills it with `rows` random rows (plus
    /// the boundary rows).
    pub async fn new(db: &TestDatabase, rows: usize) -> Fixture {
        let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        // The session settings the engine's own text parity relies on.
        client
            .batch_execute(&format!(
                "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; \
                 set extra_float_digits to 3"
            ))
            .await
            .expect("session settings");
        let pool =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

        let bases = bases();
        let ddl: Vec<String> = bases
            .iter()
            .flat_map(|b| {
                ["a", "b"]
                    .into_iter()
                    .map(move |s| format!("{}{s} {}", b.stem, b.pg))
            })
            .collect();
        client
            .batch_execute(&format!(
                "create table public.{TABLE} (id integer primary key, {})",
                ddl.join(", ")
            ))
            .await
            .expect("create the table");

        // A deterministic generator, so a failure reproduces from the same
        // rows. Row `k` starts from pool entry `k` of every column (the
        // boundary rows come first), then goes random; one in six cells is
        // NULL and row 0 is all NULL.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let names: Vec<String> = columns().into_iter().map(|(n, _)| n).collect();
        let pools: Vec<&[&str]> = bases.iter().flat_map(|b| [b.pool, b.pool]).collect();
        for id in 0..rows {
            let cells: Vec<String> = pools
                .iter()
                .enumerate()
                .map(|(col, pool)| {
                    let roll = next();
                    if id == 0 || roll % 6 == 0 {
                        return "NULL".to_string();
                    }
                    let pick = if id < pool.len() + 1 && col % 2 == 0 {
                        id - 1
                    } else {
                        (next() as usize) % pool.len()
                    };
                    format!("'{}'", pool[pick % pool.len()].replace('\'', "''"))
                })
                .collect();
            client
                .batch_execute(&format!(
                    "insert into public.{TABLE} (id, {}) values ({id}, {})",
                    names.join(", "),
                    cells.join(", ")
                ))
                .await
                .expect("insert a row");
        }

        let mut source_columns: HashMap<String, ValueType> = columns().into_iter().collect();
        source_columns.insert("id".to_string(), ValueType::Integer(IntWidth::Int4));
        Fixture {
            client,
            pool,
            source_columns,
            next_target: std::cell::Cell::new(0),
        }
    }

    fn definition_text(&self, text: &str) -> String {
        let n = self.next_target.get();
        self.next_target.set(n + 1);
        format!("TRANSFORM tgt_{n} FROM {TABLE} SELECT {text} AS v")
    }

    /// `Some(SQLSTATE)` when `sql` fails.
    async fn raises(&self, sql: &str) -> Option<String> {
        self.client.query(sql, &[]).await.err().map(|e| code_of(&e))
    }

    /// The first column of every row, as text, or the SQLSTATE.
    async fn rows(&self, sql: &str) -> Result<Vec<Option<String>>, String> {
        match self.client.query(sql, &[]).await {
            Ok(rows) => Ok(rows.iter().map(|r| r.get::<_, Option<String>>(1)).collect()),
            Err(e) => Err(code_of(&e)),
        }
    }

    /// Checks one expression `text` the generator wrote as `intended`
    /// (when it knows the tree). Every property but Refusal applies.
    pub async fn check(
        &self,
        text: &str,
        intended: Option<&Expr>,
    ) -> Result<Agreement, Divergence> {
        let diverge = |property, detail: String| Divergence {
            property,
            text: text.to_string(),
            detail,
        };
        let def_text = self.definition_text(text);
        let def = match parse(&def_text) {
            Ok(def) => def,
            Err(e) => return Err(diverge(Property::FalseRefusal, format!("parse: {e}"))),
        };
        let expr = def.fields[0].expr.clone();

        // 3. Round trip.
        if let Some(intended) = intended
            && &expr != intended
        {
            return Err(diverge(
                Property::RoundTrip,
                format!("parsed to {expr:?}, written from {intended:?}"),
            ));
        }
        let rendered_def = render_definition_text(&def);
        match parse(&rendered_def) {
            Ok(again) if again == def => {}
            Ok(again) => {
                return Err(diverge(
                    Property::RoundTrip,
                    format!("`{rendered_def}` parsed to {again:?}, not {def:?}"),
                ));
            }
            Err(e) => {
                return Err(diverge(
                    Property::RoundTrip,
                    format!("`{rendered_def}` does not parse: {e}"),
                ));
            }
        }

        // Postgres plans the user's text against no rows: a constant it
        // folds into an error raises here.
        let probe = self
            .raises(&format!("select ({text}) from {TABLE} where false"))
            .await;

        match validate(&def, &self.source_columns) {
            Err(err @ ValidationError::ConstantDivisorZero { .. }) => {
                self.check_zero_divisor_refusal(text, &expr, &err, probe)
                    .await
            }
            Err(err) if is_range_refusal(&err) => match probe {
                Some(code) if class_of(&code) == "22" => {
                    Ok(Agreement::RefusedAndRaises(err.to_string()))
                }
                Some(code) => Err(diverge(
                    Property::FalseRefusal,
                    format!("refused ({err}) but Postgres raises {code}, not class 22"),
                )),
                None => Err(diverge(
                    Property::FalseRefusal,
                    format!("refused ({err}) but Postgres plans it without error"),
                )),
            },
            Err(err) => Err(diverge(
                Property::FalseRefusal,
                format!("a well-typed expression was refused: {err}"),
            )),
            Ok(()) => {
                // Define asks Postgres to judge each literal pattern.
                if let Some(message) = self.rejected_pattern(&expr).await {
                    return Ok(Agreement::RefusedAndRaises(message));
                }
                if let Some(code) = probe {
                    let gap = if has_constant_call(&expr) {
                        Some(KNOWN_GAP_CONSTANT_CALL)
                    } else {
                        None
                    };
                    if let Some(gap) = gap {
                        return self.recheck_without_gaps(text, &expr, gap).await;
                    }
                    return Err(diverge(
                        Property::MissedRefusal,
                        format!("admitted, but Postgres raises {code} while planning it"),
                    ));
                }
                self.check_type(text, &def, &diverge).await?;
                match self.check_value(text, &expr, &diverge).await? {
                    Some(gap) => self.recheck_without_gaps(text, &expr, gap).await,
                    None => Ok(Agreement::Admitted),
                }
            }
        }
    }

    /// A refusal for a constant zero divisor. Postgres folds a constant
    /// division and raises at plan time; with a column dividend it plans the
    /// division and raises for every row that has a dividend. The refusal
    /// stands when no row of the table computes a value: each raises class
    /// 22, or is `NULL` (a `NULL` dividend) or `NaN` (a `NaN` dividend divides
    /// by zero to `NaN`).
    async fn check_zero_divisor_refusal(
        &self,
        text: &str,
        expr: &Expr,
        err: &ValidationError,
        probe: Option<String>,
    ) -> Result<Agreement, Divergence> {
        let diverge = |detail: String| Divergence {
            property: Property::FalseRefusal,
            text: text.to_string(),
            detail,
        };
        if let Some(code) = probe {
            return if class_of(&code) == "22" {
                Ok(Agreement::RefusedAndRaises(err.to_string()))
            } else {
                Err(diverge(format!(
                    "refused ({err}) but Postgres raises {code}, not class 22"
                )))
            };
        }
        match self
            .rows(&format!(
                "select id, ({text})::text from {TABLE} order by id"
            ))
            .await
        {
            Err(code) if class_of(&code) == "22" => {
                Ok(Agreement::RefusedAndRaises(err.to_string()))
            }
            Err(code) => Err(diverge(format!(
                "refused ({err}) but Postgres raises {code}, not class 22"
            ))),
            Ok(rows)
                if rows
                    .iter()
                    .all(|v| matches!(v.as_deref(), None | Some("NaN"))) =>
            {
                Ok(Agreement::RefusedAndRaises(err.to_string()))
            }
            // Postgres evaluates a `COALESCE` argument only for the rows the
            // earlier ones leave `NULL`, a `CASE` result only where its
            // condition holds, and an `AND` operand only where the earlier
            // ones allow, so the division may never run.
            // Define refuses it all the same: a zero divisor is never useful.
            Ok(_) if may_skip_evaluation(expr) => Ok(Agreement::RefusedAndRaises(err.to_string())),
            Ok(_) => Err(diverge(format!(
                "refused ({err}) but Postgres computes a value for some row"
            ))),
        }
    }

    /// 1. Type: the target column define creates has the type of `CREATE
    /// TABLE AS SELECT <text>`.
    async fn check_type(
        &self,
        text: &str,
        def: &trellis::dev::defs::ast::TransformDef,
        diverge: &impl Fn(Property, String) -> Divergence,
    ) -> Result<(), Divergence> {
        let pk = source_primary_key(&self.pool, &def.source)
            .await
            .expect("the table's key");
        let target = &def.target;
        if let Err(e) = create_target_table(
            &self.pool,
            def,
            "public",
            &pk,
            &self.source_columns,
            &def.source,
        )
        .await
        {
            return Err(diverge(
                Property::Type,
                format!("define cannot create the target: {e}"),
            ));
        }
        let ctas = format!("ctas_{target}");
        let made = self
            .client
            .batch_execute(&format!(
                "create table public.{ctas} as select ({text}) as v from {TABLE} where false"
            ))
            .await;
        let type_of = |table: String| async move {
            self.client
                .query_one(
                    "select format_type(atttypid, atttypmod) from pg_attribute \
                     where attrelid = $1::text::regclass and attname = 'v'",
                    &[&format!("public.{table}")],
                )
                .await
                .map(|r| r.get::<_, String>(0))
        };
        let result = match made {
            Err(e) => Err(diverge(
                Property::MissedRefusal,
                format!(
                    "CREATE TABLE AS fails: {}",
                    e.as_db_error()
                        .map_or_else(|| e.to_string(), ToString::to_string)
                ),
            )),
            Ok(()) => {
                let engine = type_of(target.clone()).await.expect("target column type");
                let postgres = type_of(ctas.clone()).await.expect("ctas column type");
                if engine == postgres {
                    Ok(())
                } else {
                    Err(diverge(
                        Property::Type,
                        format!("define creates `{engine}`, Postgres types it `{postgres}`"),
                    ))
                }
            }
        };
        let _ = self
            .client
            .batch_execute(&format!(
                "drop table if exists public.{ctas}; drop table if exists public.\"{target}\""
            ))
            .await;
        result
    }

    /// 2. Value: the rendered SQL and the user's text agree on every row.
    async fn check_value(
        &self,
        text: &str,
        expr: &Expr,
        diverge: &impl Fn(Property, String) -> Divergence,
    ) -> Result<Option<&'static str>, Divergence> {
        let user = self
            .rows(&format!(
                "select id, ({text})::text from {TABLE} order by id"
            ))
            .await;
        // The build's renderer, then the one a 1-1 apply and a `JOIN` write
        // embed (each column read through an alias): separate code, so each
        // is checked against the user's text.
        let mut gap = None;
        for (rendered_sql, from) in [
            (render_expr_sql(expr), TABLE.to_string()),
            (
                render_qualified_expr_sql(expr, "s"),
                format!("{TABLE} as s"),
            ),
        ] {
            let rendered = self
                .rows(&format!(
                    "select id, ({rendered_sql})::text from {from} order by id"
                ))
                .await;
            if let Some(g) = Self::compare_rows(&user, rendered, &rendered_sql, diverge)? {
                gap = Some(g);
            }
        }
        // The two renderers type the expression alike (a value's text can
        // agree across types: `cast(x as bigint)` and `cast(x as numeric)`).
        // Preparing the statement types it without running it.
        let (build, qualified) = (render_expr_sql(expr), render_qualified_expr_sql(expr, "s"));
        if let Ok(statement) = self
            .client
            .prepare(&format!(
                "select ({build}), ({qualified}) from {TABLE} as s"
            ))
            .await
        {
            let columns = statement.columns();
            if columns[0].type_() != columns[1].type_() {
                return Err(diverge(
                    Property::Value,
                    format!(
                        "`{build}` is {} but `{qualified}` is {}",
                        columns[0].type_(),
                        columns[1].type_()
                    ),
                ));
            }
        }
        Ok(gap)
    }

    /// One renderer's rows against the user's.
    fn compare_rows(
        user: &Result<Vec<Option<String>>, String>,
        rendered: Result<Vec<Option<String>>, String>,
        rendered_sql: &str,
        diverge: &impl Fn(Property, String) -> Divergence,
    ) -> Result<Option<&'static str>, Divergence> {
        match (user, rendered) {
            (Ok(u), Ok(r)) if *u == r => Ok(None),
            (Ok(u), Ok(r)) => {
                let at = u.iter().zip(&r).position(|(a, b)| a != b).unwrap_or(0);
                Err(diverge(
                    Property::Value,
                    format!(
                        "row {at}: Postgres gives {:?}, rendered `{rendered_sql}` gives {:?}",
                        u.get(at),
                        r.get(at)
                    ),
                ))
            }
            (Err(u), Err(r)) if class_of(u) == class_of(&r) => Ok(None),
            (u, r) => Err(diverge(
                Property::Value,
                format!(
                    "Postgres {}, rendered `{rendered_sql}` {}",
                    describe(u),
                    describe(&r)
                ),
            )),
        }
    }

    /// An expression that hit an open gap agrees only if the gap is all
    /// that is wrong with it: each pattern Postgres rejects becomes `'a'`,
    /// then each function call over constants becomes the literal Postgres
    /// computes for it (what define would see if it could fold the call),
    /// and the result must pass every property on its own. Otherwise a
    /// different missed refusal, or a wrong value, beside the gap would be
    /// tolerated with it.
    async fn recheck_without_gaps(
        &self,
        text: &str,
        expr: &Expr,
        gap: &'static str,
    ) -> Result<Agreement, Divergence> {
        let mut rewritten = expr.clone();
        self.fold_constant_calls(&mut rewritten).await;
        normalize_negations(&mut rewritten);
        if rewritten == *expr {
            return Err(Divergence {
                property: Property::MissedRefusal,
                text: text.to_string(),
                detail: format!("tolerated as `{gap}`, but nothing in it is that gap"),
            });
        }
        let again = crate::expr_gen::render_text(&rewritten, Default::default());
        match Box::pin(self.check(&again, Some(&rewritten))).await {
            Ok(Agreement::KnownGap(other)) => Err(Divergence {
                property: Property::MissedRefusal,
                text: text.to_string(),
                detail: format!("`{again}`, with the gaps rewritten away, still hits `{other}`"),
            }),
            Ok(_) => Ok(Agreement::KnownGap(gap)),
            Err(d) => Err(Divergence {
                detail: format!("beside the known gap `{gap}`, as `{again}`: {}", d.detail),
                text: text.to_string(),
                ..d
            }),
        }
    }

    /// The refusal define gives `expr` when a `REGEXP_COUNT` pattern literal
    /// in it is one Postgres rejects (`2201B`): the probe define runs, here
    /// run on the harness's own connection.
    async fn rejected_pattern(&self, expr: &Expr) -> Option<String> {
        let mut stack = vec![expr];
        while let Some(e) = stack.pop() {
            match e {
                Expr::BinaryOp { lhs, rhs, .. } => {
                    stack.push(lhs);
                    stack.push(rhs);
                }
                Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => stack.push(operand),
                Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
                    stack.extend(e.logic_children());
                }
                Expr::FunctionCall { name, args } => {
                    if name == "REGEXP_COUNT"
                        && let Some(Expr::StringLiteral(pattern)) = args.get(1)
                    {
                        let probe =
                            format!("select regexp_count('', '{}')", pattern.replace('\'', "''"));
                        if self.raises(&probe).await.as_deref() == Some(INVALID_REGEX) {
                            return Some(format!(
                                "regexp_count's pattern '{pattern}' is rejected by Postgres"
                            ));
                        }
                    }
                    stack.extend(args.iter());
                }
                _ => {}
            }
        }
        None
    }

    /// Replaces each function call (not `COALESCE`) that reads no column with
    /// the literal Postgres computes for it, innermost first. A call Postgres
    /// cannot compute (its constant arguments raise) is left as it is: the
    /// rest of the expression then has to explain the failure on its own, as
    /// a refusal.
    async fn fold_constant_calls(&self, expr: &mut Expr) {
        match expr {
            Expr::BinaryOp { lhs, rhs, .. } => {
                Box::pin(self.fold_constant_calls(lhs)).await;
                Box::pin(self.fold_constant_calls(rhs)).await;
            }
            Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => {
                Box::pin(self.fold_constant_calls(operand)).await;
            }
            Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
                for child in expr.logic_children_mut() {
                    Box::pin(self.fold_constant_calls(child)).await;
                }
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args.iter_mut() {
                    Box::pin(self.fold_constant_calls(arg)).await;
                }
            }
            _ => {}
        }
        let Expr::FunctionCall { name, .. } = &*expr else {
            return;
        };
        if name == "COALESCE" || name == "NULLIF" || reads_column(expr) {
            return;
        }
        let rendered = render_expr_sql(expr);
        let sql = format!("select ({rendered})::text, pg_typeof({rendered})::text");
        let Ok(row) = self.client.query_one(&sql, &[]).await else {
            return;
        };
        let value: Option<String> = row.get(0);
        let pg_type: String = row.get(1);
        // A call that gives `NULL` (over a `NULLIF` that is) has no literal
        // to stand for it: it stays, and the rest has to explain the failure.
        if value.is_none() {
            return;
        }
        *expr = match (pg_type.as_str(), value) {
            ("smallint" | "integer", Some(v)) => Expr::NumberLiteral(v),
            // A bare literal is typed by its value, so a `numeric` or
            // `bigint` result that reads as a smaller type is cast to its
            // own.
            ("bigint" | "numeric", Some(v)) => {
                let target = if pg_type == "bigint" {
                    ValueType::Integer(IntWidth::Int8)
                } else {
                    ValueType::Numeric
                };
                if crate::expr_gen::number_literal_type(&v) == target {
                    Expr::NumberLiteral(v)
                } else {
                    Expr::Cast {
                        expr: Box::new(Expr::NumberLiteral(v)),
                        target,
                    }
                }
            }
            ("text", Some(v)) => Expr::StringLiteral(v),
            ("real", Some(v)) => Expr::TypedLiteral {
                value_type: ValueType::Float(FloatWidth::Float4),
                text: v,
            },
            ("double precision", Some(v)) => Expr::TypedLiteral {
                value_type: ValueType::Float(FloatWidth::Float8),
                text: v,
            },
            (ty, v) => panic!(
                "`{sql}` of type {ty} gives {v:?}: teach fold_constant_calls to spell it as a \
                 literal"
            ),
        };
    }

    /// The Refusal property: `miss` is refused at define and the error
    /// names the construct.
    pub fn check_refusal(&self, miss: &NearMiss) -> Result<String, Divergence> {
        let diverge = |detail: String| Divergence {
            property: Property::Refusal,
            text: miss.text.clone(),
            detail: format!("[{}] {detail}", miss.kind),
        };
        let message = match parse(&self.definition_text(&miss.text)) {
            Err(e) => e.to_string(),
            Ok(def) => match validate(&def, &self.source_columns) {
                Err(e) => e.to_string(),
                Ok(()) => return Err(diverge("define accepted it".to_string())),
            },
        };
        let lower = message.to_lowercase();
        if miss.needles.iter().any(|n| lower.contains(n)) {
            Ok(message)
        } else {
            Err(diverge(format!(
                "the refusal does not name {:?}: {message}",
                miss.needles
            )))
        }
    }
}

/// Whether `expr` holds a construct that evaluates only part of itself at run
/// time: `COALESCE`, `CASE`, `AND` and `OR`.
fn may_skip_evaluation(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp {
            op: Operator::And | Operator::Or,
            ..
        } => true,
        Expr::Case { .. } => true,
        // A dividend `NULLIF` makes `NULL` never raises.
        Expr::FunctionCall { name, .. } if name == "NULLIF" => true,
        Expr::BinaryOp { lhs, rhs, .. } => may_skip_evaluation(lhs) || may_skip_evaluation(rhs),
        Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => may_skip_evaluation(operand),
        Expr::Not(_) | Expr::IsNull { .. } => {
            expr.logic_children().into_iter().any(may_skip_evaluation)
        }
        Expr::FunctionCall { name, args } => {
            name == "COALESCE" || args.iter().any(may_skip_evaluation)
        }
        _ => false,
    }
}

/// A prefix minus over a number literal is one negative literal, as the parser
/// reads it: rewrite the tree to the form its text parses back to.
fn normalize_negations(expr: &mut Expr) {
    match expr {
        Expr::BinaryOp { lhs, rhs, .. } => {
            normalize_negations(lhs);
            normalize_negations(rhs);
        }
        Expr::FunctionCall { args, .. } => args.iter_mut().for_each(normalize_negations),
        Expr::Cast { expr: operand, .. } => normalize_negations(operand),
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            expr.logic_children_mut()
                .into_iter()
                .for_each(normalize_negations);
        }
        Expr::Negate(operand) => {
            normalize_negations(operand);
            if let Expr::NumberLiteral(text) = &**operand {
                let flipped = match text.strip_prefix('-') {
                    Some(positive) => positive.to_string(),
                    None => format!("-{text}"),
                };
                *expr = Expr::NumberLiteral(flipped);
            }
        }
        _ => {}
    }
}

fn describe(result: &Result<Vec<Option<String>>, String>) -> String {
    match result {
        Ok(rows) => format!("returns {} rows", rows.len()),
        Err(code) => format!("raises {code}"),
    }
}
