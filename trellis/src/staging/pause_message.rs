//! The message of a paused definition: every reason it is paused for, and
//! what `RESUME` would do now.
//!
//! A definition can be paused for several reasons at once: a key change and
//! a column re-type in one migration, a restore while an operator's pause
//! stands. Each reason is a row of `capture_failure_reasons`: its sentence,
//! the table and columns it is about, and how to tell whether its cause is
//! still there ([`Recheck`]). The state a resume needs from a reason
//! (`restored`, `relabel_lsn`, `caused_by`) stays on `capture_failures`,
//! where a second reason never drops it, and every resume honours all of it.
//!
//! `capture_failures.error`, the message `Trellis::status` reports, is
//! [`compose`]d from the reasons, the newest first, followed by what a
//! `RESUME` would do ([`ResumeOutcome`]). A pause writes its reason
//! ([`add_reason`]) and clears the outcome, which no longer describes the
//! reasons; the staging worker's capture pass writes it again.
//!
//! # The refresh
//!
//! The pass reads the fingerprint of every relation a definition reads,
//! paused ones included, in its one fingerprint query
//! (`drift_memo::Fingerprints::read`). A paused definition whose
//! [`stamp`] differs from the one its message was written at
//! (`capture_failures.refresh_stamp`, which a pause clears) is evaluated
//! again ([`refresh_paused`]): each reason whose cause can be read from the
//! schema is checked, a column it reads that is gone now is added as a
//! reason, and the outcome is worked out as a resume would act now. Every
//! other paused definition costs nothing: its stamp is read with the
//! catalog the pass reads anyway.
//!
//! The refresh only rewrites the message. It resumes, reinstalls and
//! rebuilds nothing: a reason whose cause was fixed leaves the message, and
//! with none left the message says the cause is gone and what `RESUME` would
//! do. The definition stays paused until `RESUME`, which re-validates
//! everything define would and acts on what it finds then, never on the
//! message.
//!
//! What the fingerprint doesn't cover (a view created on the target, a
//! grant, a row-security policy, an upstream going live) doesn't refresh the
//! message; `RESUME` checks those itself and refuses naming them.

use std::collections::BTreeSet;

use tokio_postgres::{GenericClient, Transaction};

use crate::capture::CaptureError;
use crate::capture::columns::{CaptureCatalog, CaptureReader};
use crate::defs::catalog::{self, CatalogError};
use crate::defs::floor::Floors;

/// What joins one reason, ended with a full stop, to the older one after
/// it.
pub(crate) const ALREADY_PAUSED: &str = " It was already paused: ";

/// The message of a definition with no reason left: every cause it was
/// paused for was fixed while it stayed paused.
pub(crate) const CAUSE_GONE: &str = "the cause it was paused for is gone";

/// How the pass tells whether a reason's cause is still there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Recheck {
    /// It stands until a resume: a restore, a relabel, a halt, a disruption
    /// of capture, an operator's resume withdrawn.
    Stands,
    /// The reason's columns of its table are renamed or dropped; its cause
    /// is gone once the table has every one of them again.
    Columns,
    /// The table's primary key was redefined from the reason's columns; its
    /// cause is gone once the key is those columns again.
    Key,
}

impl Recheck {
    fn as_stored(self) -> Option<&'static str> {
        match self {
            Recheck::Stands => None,
            Recheck::Columns => Some("columns"),
            Recheck::Key => Some("key"),
        }
    }

    /// A word a newer release wrote reads as a reason that stands.
    fn from_stored(stored: Option<&str>) -> Recheck {
        match stored {
            Some("columns") => Recheck::Columns,
            Some("key") => Recheck::Key,
            _ => Recheck::Stands,
        }
    }
}

/// One reason a definition is paused for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reason {
    /// The qualified table it is about.
    pub(crate) source_table: String,
    /// The columns it is about, if any.
    pub(crate) columns: Vec<String>,
    /// The sentence, naming the cause and what to do.
    pub(crate) text: String,
    pub(crate) recheck: Recheck,
}

impl Reason {
    /// A reason that stands until a resume.
    pub(crate) fn standing(source_table: &str, columns: &[String], text: &str) -> Reason {
        Reason {
            source_table: source_table.to_string(),
            columns: columns.to_vec(),
            text: text.to_string(),
            recheck: Recheck::Stands,
        }
    }
}

/// One part of what a `RESUME` would do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Part {
    /// A step it takes, as a verb phrase ("rebuild ... from ...").
    Step(String),
    /// What makes it refuse.
    Refuse(String),
}

/// What a `RESUME` would do now, composed from the parts each provider adds
/// ([`resume_outcome`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ResumeOutcome {
    parts: Vec<Part>,
}

impl ResumeOutcome {
    pub(crate) fn step(&mut self, step: impl Into<String>) {
        self.parts.push(Part::Step(step.into()));
    }

    pub(crate) fn refuse(&mut self, reason: impl Into<String>) {
        self.parts.push(Part::Refuse(reason.into()));
    }

    pub(crate) fn refuses(&self) -> bool {
        self.parts.iter().any(|p| matches!(p, Part::Refuse(_)))
    }

    /// The sentence: every refusal when there is one (a refused resume
    /// takes none of its steps), otherwise every step in order.
    pub(crate) fn sentence(&self) -> String {
        let refusals: Vec<&str> = self
            .parts
            .iter()
            .filter_map(|p| match p {
                Part::Refuse(r) => Some(r.as_str()),
                Part::Step(_) => None,
            })
            .collect();
        if !refusals.is_empty() {
            return format!("RESUME would refuse: {}", refusals.join("; and "));
        }
        let steps: Vec<&str> = self
            .parts
            .iter()
            .filter_map(|p| match p {
                Part::Step(s) => Some(s.as_str()),
                Part::Refuse(_) => None,
            })
            .collect();
        if steps.is_empty() {
            return "RESUME would rebuild it".to_string();
        }
        format!("RESUME would {}", steps.join(", then "))
    }
}

/// The message of a definition paused for `reasons` (oldest first), with
/// what a `RESUME` would do, when the pass has worked it out: the newest
/// reason first, each older one after it, then the outcome.
pub(crate) fn compose(reasons: &[Reason], outcome: Option<&str>) -> String {
    let mut sentences: Vec<&str> = reasons.iter().rev().map(|r| r.text.as_str()).collect();
    if sentences.is_empty() {
        sentences.push(CAUSE_GONE);
    }
    let mut message = String::new();
    for (i, sentence) in sentences.iter().enumerate() {
        if i > 0 {
            message.push('.');
            message.push_str(ALREADY_PAUSED);
        }
        message.push_str(if i + 1 < sentences.len() || outcome.is_some() {
            sentence.trim_end_matches('.')
        } else {
            sentence
        });
    }
    if let Some(outcome) = outcome {
        message.push_str(". ");
        message.push_str(outcome);
    }
    message
}

/// [`compose`] over reason sentences listed the newest first, as
/// `Trellis::status` reads them.
pub(crate) fn compose_texts(newest_first: &[String], outcome: Option<&str>) -> String {
    let reasons: Vec<Reason> = newest_first
        .iter()
        .rev()
        .map(|text| Reason::standing("", &[], text))
        .collect();
    compose(&reasons, outcome)
}

/// A definition's stored message: its reasons, oldest first, and its
/// outcome.
#[derive(Debug, Clone)]
struct Stored {
    reasons: Vec<Reason>,
    outcome: Option<String>,
    /// `capture_failures.error` as read.
    error: String,
}

/// Reads definition `id`'s reasons and outcome, or `None` without a
/// `capture_failures` row. A message this release didn't compose from the
/// rows (written by an older release, or before its reasons were recorded)
/// reads as one reason that stands, so nothing it says is lost.
async fn load(client: &impl GenericClient, id: i64) -> Result<Option<Stored>, CatalogError> {
    let Some(row) = client
        .query_opt(
            "select error, resume_outcome, source_table, columns from capture_failures \
             where transform_id = $1",
            &[&id],
        )
        .await?
    else {
        return Ok(None);
    };
    let error: String = row.get(0);
    let outcome: Option<String> = row.get(1);
    let reasons: Vec<Reason> = client
        .query(
            "select source_table, columns, reason, recheck from capture_failure_reasons \
             where transform_id = $1 order by position",
            &[&id],
        )
        .await?
        .into_iter()
        .map(|r| Reason {
            source_table: r.get(0),
            columns: r.get(1),
            text: r.get(2),
            recheck: Recheck::from_stored(r.get(3)),
        })
        .collect();
    if compose(&reasons, outcome.as_deref()) == error {
        return Ok(Some(Stored {
            reasons,
            outcome,
            error,
        }));
    }
    let source_table: String = row.get(2);
    let columns: Vec<String> = row.get(3);
    Ok(Some(Stored {
        reasons: vec![Reason::standing(&source_table, &columns, &error)],
        outcome: None,
        error,
    }))
}

/// Writes definition `id`'s `reasons` (oldest first) and `outcome`, and
/// the message composed from them. The record's table and columns are the
/// oldest reason's, as the first detection's always were. `stamp` is what
/// the pass evaluated them at; `None` has the next pass evaluate them.
async fn write(
    client: &impl GenericClient,
    id: i64,
    reasons: &[Reason],
    outcome: Option<&str>,
    stamp: Option<&str>,
) -> Result<(), CatalogError> {
    client
        .execute(
            "delete from capture_failure_reasons where transform_id = $1",
            &[&id],
        )
        .await?;
    for (position, reason) in (1_i32..).zip(reasons) {
        client
            .execute(
                "insert into capture_failure_reasons \
                     (transform_id, position, source_table, columns, reason, recheck) \
                 values ($1, $2, $3, $4, $5, $6)",
                &[
                    &id,
                    &position,
                    &reason.source_table,
                    &reason.columns,
                    &reason.text,
                    &reason.recheck.as_stored(),
                ],
            )
            .await?;
    }
    let first = reasons.first();
    client
        .execute(
            "update capture_failures \
             set error = $2, resume_outcome = $3, refresh_stamp = $4, \
                 source_table = coalesce($5, source_table), \
                 columns = coalesce($6, '{}'::text[]) \
             where transform_id = $1",
            &[
                &id,
                &compose(reasons, outcome),
                &outcome,
                &stamp,
                &first.map(|r| r.source_table.as_str()),
                &first.map(|r| r.columns.clone()),
            ],
        )
        .await?;
    Ok(())
}

/// The placeholder message a new `capture_failures` row is inserted with
/// before its first reason is added ([`add_reason`]): what [`compose`]
/// gives for no reasons, so [`load`] reads the row as having none.
pub(crate) const NO_REASON_YET: &str = CAUSE_GONE;

/// Adds `reason` to definition `id`'s `capture_failures` record, which the
/// caller has inserted or locked in `txn`, unless it already has a reason
/// with the same sentence (a peer repeating the pause). Clears the outcome,
/// which the next capture pass works out again for the reasons as they are
/// now. Returns whether it added it.
pub(crate) async fn add_reason(
    txn: &Transaction<'_>,
    id: i64,
    reason: Reason,
) -> Result<bool, CatalogError> {
    let Some(mut stored) = load(txn, id).await? else {
        return Ok(false);
    };
    if stored.reasons.iter().any(|r| r.text == reason.text) {
        return Ok(false);
    }
    stored.reasons.push(reason);
    write(txn, id, &stored.reasons, None, None).await?;
    Ok(true)
}

/// Replaces every reason of definition `id`'s record with `reasons`: what
/// a resume in progress, or the end of one, reports, and the reason an
/// already-recorded reason of the same kind gives way to.
pub(crate) async fn replace_reasons(
    client: &impl GenericClient,
    id: i64,
    reasons: &[Reason],
) -> Result<(), CatalogError> {
    write(client, id, reasons, None, None).await
}

/// Rewrites the reasons of definition `id` with `rewrite`, which gets them
/// oldest first; clears the outcome when they change. Returns whether they
/// did.
pub(crate) async fn rewrite_reasons(
    txn: &Transaction<'_>,
    id: i64,
    rewrite: impl FnOnce(&mut Vec<Reason>),
) -> Result<bool, CatalogError> {
    let Some(mut stored) = load(txn, id).await? else {
        return Ok(false);
    };
    let before = stored.reasons.clone();
    rewrite(&mut stored.reasons);
    if stored.reasons == before {
        return Ok(false);
    }
    write(txn, id, &stored.reasons, None, None).await?;
    Ok(true)
}

/// The stamp a paused `reader`'s message is written at: a digest of the
/// definition and the fingerprints of every relation it reads
/// (`drift_memo::Fingerprints::stamp`). `None` when a fingerprint is
/// missing, which has the pass evaluate it.
pub(crate) fn stamp(
    fingerprints: &super::drift_memo::Fingerprints,
    schema: &str,
    reader: &CaptureReader,
) -> Option<String> {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let stamp = fingerprints.stamp(schema, reader)?;
    let mut hasher = DefaultHasher::new();
    stamp.hash(&mut hasher);
    Some(format!("{:016x}", hasher.finish()))
}

/// The capture pass's refresh (see the module doc): evaluates again every
/// definition of `catalog` paused with a record whose stamp differs from
/// the one its message was written at. Returns how many it evaluated.
pub(crate) async fn refresh_paused(
    client: &mut tokio_postgres::Client,
    schema: &str,
    floors: &Floors,
    instance: &str,
    catalog: &CaptureCatalog,
    fingerprints: &super::drift_memo::Fingerprints,
) -> Result<usize, CaptureError> {
    let mut evaluated = 0;
    for reader in catalog.definitions.iter().filter(|r| r.capture_failed) {
        let stamp = stamp(fingerprints, schema, reader);
        if stamp.is_some() && stamp == reader.refresh_stamp {
            continue;
        }
        refresh(client, schema, floors, reader, stamp.as_deref()).await?;
        evaluated += 1;
        #[cfg(any(test, feature = "internals"))]
        probe::count(instance, reader.id);
    }
    let _ = instance;
    Ok(evaluated)
}

/// Evaluates paused `reader`'s reasons and outcome again, and writes them
/// with `stamp`, unless its record changed meanwhile, when the next pass
/// evaluates it instead. Its record is read and evaluated in a transaction
/// that changes nothing, and written in another under the record's row
/// lock: nothing is locked while a resume is worked out.
async fn refresh(
    client: &mut tokio_postgres::Client,
    schema: &str,
    floors: &Floors,
    reader: &CaptureReader,
    stamp: Option<&str>,
) -> Result<(), CaptureError> {
    let id = reader.id;
    let txn = client.transaction().await?;
    let Some(stored) = load(&txn, id).await? else {
        return Ok(());
    };
    let requested: bool = txn
        .query_one(
            "select exists (select 1 from resume_requests where transform_id = $1)",
            &[&id],
        )
        .await?
        .get(0);
    // A definition this release can't run (ADR-0024) refuses every
    // `RESUME` here, as its reason says, with what to do instead.
    let unrunnable = match catalog::refuse_unrunnable(&txn, floors, &reader.target, "resume").await
    {
        Ok(()) => false,
        Err(err @ (CatalogError::Db(_) | CatalogError::Pool(_))) => return Err(err.into()),
        Err(_) => true,
    };
    let (reasons, outcome) = if requested || unrunnable {
        // A resume in progress reports itself, and its end writes a reason.
        (stored.reasons.clone(), stored.outcome.clone())
    } else {
        let reasons = recheck(&txn, reader, &stored.reasons).await?;
        let outcome = resume_outcome(&txn, schema, floors, id).await?;
        (reasons, outcome.map(|o| o.sentence()))
    };
    txn.rollback().await?;

    let txn = client.transaction().await?;
    let current: Option<String> = txn
        .query_opt(
            "select error from capture_failures where transform_id = $1 for update",
            &[&id],
        )
        .await?
        .map(|row| row.get(0));
    if current.as_deref() == Some(stored.error.as_str()) {
        write(&txn, id, &reasons, outcome.as_deref(), stamp).await?;
    }
    txn.commit().await?;
    Ok(())
}

/// `reasons` (oldest first) as the schema stands now: each whose cause is
/// gone is dropped, each whose cause changed says what it is now, and a
/// column `reader` reads that a table it reads no longer has is added as a
/// reason after them. A table that doesn't exist now leaves its reasons as
/// they are: the reason it is gone stands on its own.
async fn recheck(
    client: &impl GenericClient,
    reader: &CaptureReader,
    reasons: &[Reason],
) -> Result<Vec<Reason>, CaptureError> {
    let mut tables: BTreeSet<&str> = reasons
        .iter()
        .filter(|r| r.recheck != Recheck::Stands)
        .map(|r| r.source_table.as_str())
        .collect();
    tables.insert(reader.source.as_str());
    tables.extend(reader.join.as_deref());
    let mut live = std::collections::HashMap::new();
    for table in tables {
        let columns = crate::capture::columns::live_columns(client, table).await?;
        live.insert(table.to_string(), columns);
    }
    Ok(recheck_against(reader, reasons, |table| {
        live.get(table)
            .filter(|(attnums, _)| !attnums.is_empty())
            .map(|(attnums, key)| (attnums.keys().cloned().collect(), key.clone()))
    }))
}

/// [`recheck`] over `live`, which gives a table's column names and primary
/// key now, or `None` when it doesn't exist.
fn recheck_against(
    reader: &CaptureReader,
    reasons: &[Reason],
    live: impl Fn(&str) -> Option<(BTreeSet<String>, Vec<String>)>,
) -> Vec<Reason> {
    let mut out: Vec<Reason> = Vec::with_capacity(reasons.len());
    for reason in reasons {
        let Some((present, key)) = live(&reason.source_table) else {
            out.push(reason.clone());
            continue;
        };
        let table = reason.source_table.as_str();
        match reason.recheck {
            Recheck::Stands => out.push(reason.clone()),
            Recheck::Columns => {
                let still: Vec<String> = reason
                    .columns
                    .iter()
                    .filter(|c| !present.contains(*c))
                    .cloned()
                    .collect();
                if still.is_empty() {
                    continue;
                }
                let text = if still == reason.columns {
                    reason.text.clone()
                } else {
                    super::schema_change::missing_columns_error(table, &still)
                };
                out.push(Reason {
                    columns: still,
                    text,
                    ..reason.clone()
                });
            }
            Recheck::Key => {
                if key == reason.columns {
                    continue;
                }
                let mut reason = reason.clone();
                if !key.is_empty() {
                    reason.text =
                        super::schema_change::key_changed_error(table, &reason.columns, &key);
                }
                out.push(reason);
            }
        }
    }
    let tables: Vec<&str> = std::iter::once(reader.source.as_str())
        .chain(reader.join.as_deref())
        .collect();
    for table in tables {
        let Some((present, _)) = live(table) else {
            continue;
        };
        let Some(reads) = reader.reads(table) else {
            continue;
        };
        let named: BTreeSet<&String> = out
            .iter()
            .filter(|r| r.source_table == table && r.recheck == Recheck::Columns)
            .flat_map(|r| r.columns.iter())
            .collect();
        let gone: Vec<String> = reads
            .iter()
            .filter(|c| !present.contains(*c) && !named.contains(c))
            .cloned()
            .collect();
        if !gone.is_empty() {
            out.push(Reason {
                source_table: table.to_string(),
                text: super::schema_change::missing_columns_error(table, &gone),
                columns: gone,
                recheck: Recheck::Columns,
            });
        }
    }
    out
}

/// What a `RESUME` of definition `id` would do now, or `None` if it is
/// gone. Each provider below adds the parts its state gives, in the order
/// a resume acts (`staging::quarantine::resume_locked`); a new kind of
/// pause whose resume does something of its own adds a provider here.
/// Reads only.
async fn resume_outcome(
    txn: &Transaction<'_>,
    schema: &str,
    floors: &Floors,
    id: i64,
) -> Result<Option<ResumeOutcome>, CatalogError> {
    let Some(definition) = catalog::definition_by_id_in(txn, floors, id).await? else {
        return Ok(None);
    };
    let mut outcome = ResumeOutcome::default();
    // Define's validation against the schema as it stands, and the tables
    // Trellis owns for the definition.
    let revalidated = match catalog::revalidate(txn, schema, floors, &definition).await {
        Ok(revalidated) => Some(revalidated),
        Err(err @ (CatalogError::Db(_) | CatalogError::Pool(_))) => return Err(err),
        Err(reason) => {
            outcome.refuse(refusal(&reason));
            None
        }
    };
    if let Some(revalidated) = &revalidated {
        match catalog::check_owned_tables(txn, schema, &definition, revalidated).await {
            Ok(()) => {}
            Err(err @ (CatalogError::Db(_) | CatalogError::Pool(_))) => return Err(err),
            Err(reason) => outcome.refuse(refusal(&reason)),
        }
    }
    if outcome.refuses() {
        return Ok(Some(outcome));
    }
    let state = txn
        .query_opt(
            "select restored, relabel_lsn is not null from capture_failures \
             where transform_id = $1",
            &[&id],
        )
        .await?;
    let (restored, relabelled) = state.map_or((false, false), |row| (row.get(0), row.get(1)));
    // A relabel's: the changes staged with the old label drain first.
    if relabelled {
        outcome.step(
            "wait for the changes staged before the enum relabel or re-type to drain, since \
             they name the old label",
        );
    }
    // Typed copies whose type define would give differently now.
    let copies = crate::defs::copies::typed_copies(
        txn,
        schema,
        &definition.def,
        &definition.source_table,
        &definition.target_table,
    )
    .await?;
    let drifted: Vec<String> = crate::defs::copies::inspect(txn, copies)
        .await?
        .into_iter()
        .filter(|s| s.drifted() && !s.copy.kind.recreated_by_resume())
        .map(|s| {
            format!(
                "{} from {} to {}",
                s.copy.label(),
                s.copy_type.display,
                s.live_type.display
            )
        })
        .collect();
    if !drifted.is_empty() {
        outcome.step(format!(
            "re-type the columns Trellis created to the types define would give them now ({})",
            drifted.join(", ")
        ));
    }
    // A restore's: the ordering state the dump brought is reset.
    if restored {
        outcome.step("reset the ordering state the dump brought from the original database");
    }
    outcome.step(format!(
        "rebuild {} from {}",
        definition.target_table,
        match catalog::join_table_of(txn, id).await? {
            Some(joined) => format!("{} and {joined}", definition.source_table),
            None => definition.source_table.clone(),
        }
    ));
    Ok(Some(outcome))
}

/// The sentence of a resume refusal: define's own, fixed in the schema, or
/// one only a resume makes.
fn refusal(reason: &CatalogError) -> String {
    if reason.is_define_refusal() {
        format!("define would refuse the definition as the schema stands now: {reason}")
    } else {
        reason.to_string()
    }
}

/// How many times the pass has evaluated a paused definition's message, for
/// tests that pin the skip.
#[cfg(any(test, feature = "internals"))]
mod probe {
    use std::collections::BTreeMap;
    use std::sync::{Mutex, PoisonError};

    static REFRESHES: Mutex<BTreeMap<(String, i64), u64>> = Mutex::new(BTreeMap::new());

    pub(super) fn count(instance: &str, id: i64) {
        *REFRESHES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((instance.to_string(), id))
            .or_default() += 1;
    }

    pub(super) fn get(instance: &str, id: i64) -> u64 {
        REFRESHES
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&(instance.to_string(), id))
            .copied()
            .unwrap_or(0)
    }
}

/// The number of times the capture pass has evaluated the message of paused
/// definition `id` of the instance with schema `schema` in database
/// `database`.
#[cfg(any(test, feature = "internals"))]
pub fn refreshes(database: &str, schema: &str, id: i64) -> u64 {
    probe::get(
        &crate::capture::reconcile::instance_key(database, schema),
        id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reason(text: &str) -> Reason {
        Reason::standing("public.s", &[], text)
    }

    /// Every reason shows, the newest first, then what `RESUME` would do; a
    /// reason ending in a full stop doesn't get a second.
    #[test]
    fn the_message_joins_every_reason_newest_first_then_the_outcome() {
        let restored = reason("the database was restored from a logical dump");
        let dropped = reason("column \"b\" of public.s was renamed or dropped.");
        assert_eq!(
            compose(std::slice::from_ref(&restored), None),
            restored.text
        );
        assert_eq!(
            compose(&[dropped.clone(), restored.clone()], None),
            "the database was restored from a logical dump. It was already paused: column \"b\" \
             of public.s was renamed or dropped."
        );
        let mut outcome = ResumeOutcome::default();
        outcome.step("reset the ordering state");
        outcome.step("rebuild public.t from public.s");
        assert_eq!(
            compose(std::slice::from_ref(&dropped), Some(&outcome.sentence())),
            "column \"b\" of public.s was renamed or dropped. RESUME would reset the ordering \
             state, then rebuild public.t from public.s"
        );
        assert_eq!(
            compose(&[restored, dropped], Some("RESUME would rebuild it")),
            "column \"b\" of public.s was renamed or dropped. It was already paused: the \
             database was restored from a logical dump. RESUME would rebuild it"
        );
    }

    /// With every cause fixed, the message says so and what `RESUME` would
    /// do.
    #[test]
    fn no_reason_left_says_the_cause_is_gone() {
        assert_eq!(compose(&[], None), CAUSE_GONE);
        assert_eq!(
            compose(&[], Some("RESUME would rebuild public.t from public.s")),
            "the cause it was paused for is gone. RESUME would rebuild public.t from public.s"
        );
    }

    /// A refusal takes the place of every step, and every refusal is named.
    #[test]
    fn a_refusal_replaces_the_steps() {
        let mut outcome = ResumeOutcome::default();
        outcome.step("rebuild public.t from public.s");
        outcome.refuse("a view depends on column \"b\"");
        outcome.refuse("column \"c\" collides");
        assert!(outcome.refuses());
        assert_eq!(
            outcome.sentence(),
            "RESUME would refuse: a view depends on column \"b\"; and column \"c\" collides"
        );
        assert_eq!(
            ResumeOutcome::default().sentence(),
            "RESUME would rebuild it"
        );
    }

    /// The status side reads the reasons the newest first and composes the
    /// same message.
    #[test]
    fn composing_from_texts_matches_composing_from_reasons() {
        let reasons = [reason("first"), reason("second")];
        let texts = vec!["second".to_string(), "first".to_string()];
        assert_eq!(
            compose_texts(&texts, Some("RESUME would rebuild it")),
            compose(&reasons, Some("RESUME would rebuild it"))
        );
    }

    fn reader(text: &str) -> CaptureReader {
        CaptureReader {
            id: 1,
            source: "public.s".to_string(),
            def: crate::defs::parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}")),
            capture_failed: true,
            refresh_stamp: None,
            target: "public.t".to_string(),
            join: None,
        }
    }

    fn table(columns: &[&str], key: &[&str]) -> Option<(BTreeSet<String>, Vec<String>)> {
        Some((
            columns.iter().map(|c| c.to_string()).collect(),
            key.iter().map(|c| c.to_string()).collect(),
        ))
    }

    fn rechecked(name: &str, columns: &[&str], recheck: Recheck) -> Reason {
        let columns: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        Reason {
            source_table: "public.s".to_string(),
            text: name.to_string(),
            columns,
            recheck,
        }
    }

    /// A missing column restored, or a key put back, drops its reason; a
    /// reason that stands stays; a key changed again rewrites its reason.
    #[test]
    fn a_fixed_cause_leaves_and_a_changed_one_is_rewritten() {
        let reader = reader("TRANSFORM t FROM s SELECT id AS id, b AS b");
        let restored = rechecked("restored", &[], Recheck::Stands);
        let missing = rechecked("b missing", &["b"], Recheck::Columns);
        let key = rechecked("key changed", &["id"], Recheck::Key);
        let reasons = [restored.clone(), missing.clone(), key.clone()];

        let fixed = recheck_against(&reader, &reasons, |_| table(&["id", "b", "g"], &["id"]));
        assert_eq!(fixed, vec![restored.clone()]);

        let unchanged = recheck_against(&reader, &reasons, |_| table(&["id", "g"], &["id", "g"]));
        assert_eq!(unchanged[..2], [restored.clone(), missing.clone()]);
        assert_eq!(
            unchanged[2].text,
            super::super::schema_change::key_changed_error(
                "public.s",
                &["id".to_string()],
                &["id".to_string(), "g".to_string()]
            )
        );
        assert_eq!(unchanged[2].columns, vec!["id".to_string()]);

        // A table that is gone leaves every reason as it is.
        assert_eq!(recheck_against(&reader, &reasons, |_| None), reasons);
    }

    /// A column the definition reads that is gone now is a new reason,
    /// after the ones it already has; one a reason already names is not
    /// added twice.
    #[test]
    fn a_column_gone_while_paused_is_added_as_a_reason() {
        let reader = reader("TRANSFORM t FROM s SELECT id AS id, b AS b, c AS c");
        let restored = rechecked("restored", &[], Recheck::Stands);
        let found = recheck_against(&reader, std::slice::from_ref(&restored), |_| {
            table(&["id"], &["id"])
        });
        assert_eq!(found.len(), 2);
        assert_eq!(found[0], restored);
        assert_eq!(found[1].recheck, Recheck::Columns);
        assert_eq!(found[1].columns, vec!["b".to_string(), "c".to_string()]);

        let missing_b = rechecked("b missing", &["b"], Recheck::Columns);
        let found = recheck_against(&reader, std::slice::from_ref(&missing_b), |_| {
            table(&["id"], &["id"])
        });
        assert_eq!(found[0], missing_b);
        assert_eq!(found[1].columns, vec!["c".to_string()]);

        // Only `c` back: the reason names what is still missing.
        let both = rechecked("b, c missing", &["b", "c"], Recheck::Columns);
        let found = recheck_against(&reader, &[both], |_| table(&["id", "c"], &["id"]));
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].columns, vec!["b".to_string()]);
        assert_eq!(
            found[0].text,
            super::super::schema_change::missing_columns_error("public.s", &["b".to_string()])
        );
    }

    #[test]
    fn a_recheck_word_round_trips_and_an_unknown_one_stands() {
        for recheck in [Recheck::Stands, Recheck::Columns, Recheck::Key] {
            assert_eq!(Recheck::from_stored(recheck.as_stored()), recheck);
        }
        assert_eq!(Recheck::from_stored(Some("newer")), Recheck::Stands);
    }
}
