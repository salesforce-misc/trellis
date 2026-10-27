//! Runs single rows through the real 1-1 apply path and compares what
//! Postgres computed with independently written SQL.
//!
//! Postgres evaluates every 1-1 field, in the upsert that writes the target
//! (ADR-0002). The value of a parity test is therefore the whole path: the
//! staged image's text cast back to the column's type, the rendered
//! expression, and the target column's text. Each [`Lab::apply`] stages one
//! row as its own sealed batch and drains it once, so a row that raises a
//! data exception (SQLSTATE class 22) is reported as such without disturbing
//! the others.

#![allow(dead_code)]

use std::collections::HashMap;

use testkit::TestDatabase;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_definition, create_target_table, parse, source_primary_key};
use trellis::staging::apply::{self, ApplyError};

/// A raw connection with the session settings the engine's own text parity
/// relies on.
pub async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; \
             set extra_float_digits to 3"
        ))
        .await
        .expect("set search_path");
    client
}

pub async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    // A full ring answers `RingFull` until drained segments are retired,
    // exactly as the engine's own seal-on-demand handles it.
    let outcome = match seal::seal_phase1(client).await {
        Ok(outcome) => outcome,
        Err(trellis::StagingError::RingFull { .. }) => {
            trellis::staging::retire_drained_segments(client)
                .await
                .expect("retire drained segments");
            seal::seal_phase1(client).await.expect("seal phase 1")
        }
        Err(err) => panic!("seal phase 1: {err:?}"),
    };
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// Creates `public.<table>` with `ddl`'s columns, the 1-1 definition `text`
/// over it and its target, with `columns` the source's types.
pub async fn define(
    db: &TestDatabase,
    client: &Client,
    table: &str,
    ddl: &str,
    text: &str,
    columns: &[(&str, ValueType)],
) {
    client
        .batch_execute(&format!("create table public.{table} ({ddl})"))
        .await
        .expect("create the source");
    let columns: HashMap<String, ValueType> = columns
        .iter()
        .map(|(name, t)| (name.to_string(), *t))
        .collect();
    create_definition(&db.pool, text, &columns)
        .await
        .expect("create the definition");
    let def = parse(text).expect("parse");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("the source's key");
    create_target_table(&db.pool, &def, "public", &pk, &columns, &def.source)
        .await
        .expect("create the target");
}

/// Stages an insert of every `(key, image)` of `rows` from `public.<table>`
/// into the ring, in one statement, and seals them into one batch. Each image
/// is a jsonb object of the row's columns as text, as capture writes it.
pub async fn stage(client: &mut Client, table: &str, rows: &[(String, String)]) -> i64 {
    let (keys, images): (Vec<&str>, Vec<&str>) = rows
        .iter()
        .map(|(key, image)| (key.as_str(), image.as_str()))
        .unzip();
    let slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("the active ring slot")
        .get(0);
    client
        .execute(
            &format!(
                "insert into seg_{slot} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 select $1, k, 'insert', pg_current_wal_insert_lsn(), null, i::jsonb, 0 \
                 from unnest($2::text[], $3::text[]) as u(k, i)"
            ),
            &[&format!("public.{table}"), &keys, &images],
        )
        .await
        .expect("stage the page");
    seal_active_segment(client).await
}

/// Inserts `rows` into the source too, so a recompute of the target from
/// its source agrees with what was staged.
pub async fn insert_source(client: &Client, table: &str, rows: &[(String, String)]) {
    let images: Vec<&str> = rows.iter().map(|(_, image)| image.as_str()).collect();
    client
        .execute(
            &format!(
                "insert into public.{table} \
                 select (jsonb_populate_record(null::public.{table}, i::jsonb)).* \
                 from unnest($1::text[]) as u(i)"
            ),
            &[&images],
        )
        .await
        .expect("insert the source rows");
}

pub async fn drain(
    db: &TestDatabase,
    seg_seq: i64,
) -> Result<Option<apply::ApplyOutcome>, ApplyError> {
    apply::drain_once(&db.pool, seg_seq, "worker", 1, "trellis_render_lab_test").await
}

/// Whether `err` is a data exception (SQLSTATE class 22).
pub fn is_data_exception(err: &ApplyError) -> bool {
    match err {
        ApplyError::Db(err) => err.code().is_some_and(|code| code.code().starts_with("22")),
        _ => false,
    }
}

/// What applying one row produced.
#[derive(Debug, PartialEq)]
pub enum Outcome {
    /// The target row's fields, as text, in the definition's field order.
    Values(Vec<Option<String>>),
    /// Postgres raised a data exception; its SQLSTATE and message.
    Raised { code: String, message: String },
}

/// A source table `s` with a 1-1 definition over it, and the means to apply
/// one row at a time.
pub struct Lab<'a> {
    db: &'a TestDatabase,
    client: Client,
    fields: Vec<String>,
    target: String,
    next_key: i32,
}

impl<'a> Lab<'a> {
    /// Creates `public.s (id integer primary key, <ddl>)`, the definition
    /// `transform` (over `s`) and its target. `columns` types the source's
    /// columns, `id` included.
    pub async fn new(
        db: &'a TestDatabase,
        ddl: &str,
        transform: &str,
        columns: &[(&str, ValueType)],
    ) -> Lab<'a> {
        let client = connect_raw(db.dsn()).await;
        define(
            db,
            &client,
            "s",
            &format!("id integer primary key, {ddl}"),
            transform,
            columns,
        )
        .await;
        let def = parse(transform).expect("parse");
        Lab {
            db,
            client,
            fields: def.fields.iter().map(|f| f.name.clone()).collect(),
            target: def.target.clone(),
            next_key: 0,
        }
    }

    /// The raw connection, for the independent SQL a test compares against.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Inserts a row of `values` (column, text; `None` is SQL `NULL`) into
    /// the source, stages it as an insert and drains it once.
    pub async fn apply(&mut self, values: &[(&str, Option<&str>)]) -> Outcome {
        self.apply_with(values, true).await
    }

    /// Like [`Self::apply`], but stages the image without inserting the row
    /// into the source: for an image no source column could hold.
    pub async fn apply_image_only(&mut self, values: &[(&str, Option<&str>)]) -> Outcome {
        self.apply_with(values, false).await
    }

    async fn apply_with(&mut self, values: &[(&str, Option<&str>)], insert: bool) -> Outcome {
        self.next_key += 1;
        let id = self.next_key;
        let mut image = serde_json::Map::new();
        image.insert("id".to_string(), serde_json::Value::String(id.to_string()));
        for (column, value) in values {
            image.insert(
                column.to_string(),
                value.map_or(serde_json::Value::Null, |v| {
                    serde_json::Value::String(v.to_string())
                }),
            );
        }
        let rows = vec![(id.to_string(), serde_json::Value::Object(image).to_string())];
        if insert {
            insert_source(&self.client, "s", &rows).await;
        }
        let seg_seq = stage(&mut self.client, "s", &rows).await;
        match drain(self.db, seg_seq).await {
            Ok(_) => {}
            Err(err) if is_data_exception(&err) => {
                let ApplyError::Db(err) = err else {
                    unreachable!()
                };
                let db_err = err.as_db_error().expect("a server error");
                // The batch stays undrained, and four of them would fill the
                // ring: discard it, since only the verdict matters here.
                self.client
                    .execute(
                        "update segments set state = 'drained' where seg_seq = $1",
                        &[&seg_seq],
                    )
                    .await
                    .expect("discard the failed batch");
                trellis::staging::retire_drained_segments(&mut self.client)
                    .await
                    .expect("retire the failed batch");
                return Outcome::Raised {
                    code: db_err.code().code().to_string(),
                    message: db_err.message().to_string(),
                };
            }
            Err(err) => panic!("drain failed with something other than a data exception: {err:?}"),
        }
        let select = self
            .fields
            .iter()
            .map(|f| format!("\"{f}\"::text"))
            .collect::<Vec<_>>()
            .join(", ");
        let row = self
            .client
            .query_one(
                &format!(
                    "select {select} from public.\"{}\" where id = $1",
                    self.target
                ),
                &[&id],
            )
            .await
            .expect("read the target row");
        Outcome::Values((0..self.fields.len()).map(|i| row.get(i)).collect())
    }
}
