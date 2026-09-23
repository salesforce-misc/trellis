//! The bounded transaction buffer (issue #8): a source transaction's changes
//! are held in memory only up to a threshold, past which they spill to an
//! append-only temp file, so intake's memory is O(chunk), never
//! O(transaction) — see "Bounding memory: the whole-transaction problem" in
//! docs/staging-and-claiming/01-intake-and-lsn-confirmation.md.
//!
//! This is the spill-file path only. The structural fix — the protocol's own
//! streaming mode (`pgoutput` v2), where the server chunks a large
//! transaction before commit — is deferred: the target minimum PostgreSQL
//! version is still open, and v2 needs its own provisional-staging-by-xid
//! machinery. [`TxnBuffer`] is written so that seam can slot in later without
//! disturbing the linchpin: whatever produces `StagedChange`s for one
//! transaction, staging them is "chunks, then the tail, in one transaction."
//!
//! **Cross-chunk coalescing is not attempted here.** Two chunks' rows for one
//! key both land in the ring; the claim-time fold (stage 04) collapses them.
//! That is the correctness argument for the whole design: the fold is the
//! only place merging happens, so replaying spilled chunks in the order they
//! were written produces a result identical to a fully-buffered transaction.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use super::error::IntakeError;
use crate::staging::append::{self, CdcOp, StagedChange};

/// Default spill threshold: past this many buffered changes, the head spills
/// to disk. 256k per docs/staging-and-claiming/01-intake-and-lsn-confirmation.md.
pub const DEFAULT_SPILL_THRESHOLD: usize = 256_000;

/// Default hard cap: past this many total changes in one transaction, intake
/// refuses rather than growing without bound. 5M per the design doc.
pub const DEFAULT_HARD_CAP: usize = 5_000_000;

static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

/// One transaction's changes: an in-memory head, plus an optional spill file
/// for everything flushed out of it once the head hits `spill_threshold`.
/// `hard_cap` is checked on every push, independent of the spill threshold —
/// spilling bounds memory, but an unbounded transaction still needs a floor
/// somewhere.
pub struct TxnBuffer {
    head: Vec<StagedChange>,
    spill: Option<SpillFile>,
    spill_threshold: usize,
    hard_cap: usize,
    count: usize,
    tables: std::collections::BTreeSet<String>,
}

impl TxnBuffer {
    pub fn new(spill_threshold: usize, hard_cap: usize) -> Self {
        Self {
            head: Vec::new(),
            spill: None,
            spill_threshold,
            hard_cap,
            count: 0,
            tables: std::collections::BTreeSet::new(),
        }
    }

    /// Buffers one decoded change, spilling the head to disk if it just hit
    /// `spill_threshold`. `xid` is only used to name the spill file and the
    /// hard-cap error — it plays no role in staging order or content.
    pub fn push(&mut self, change: StagedChange, xid: u32) -> Result<(), IntakeError> {
        self.count += 1;
        if self.count > self.hard_cap {
            return Err(IntakeError::TransactionTooLarge {
                xid,
                cap: self.hard_cap,
                tables: self.tables.iter().cloned().collect(),
            });
        }
        self.tables.insert(change.src_table().to_string());
        self.head.push(change);
        if self.head.len() >= self.spill_threshold {
            self.flush_head_to_spill(xid)?;
        }
        Ok(())
    }

    fn flush_head_to_spill(&mut self, xid: u32) -> Result<(), IntakeError> {
        if self.spill.is_none() {
            self.spill = Some(SpillFile::create(xid)?);
        }
        self.spill
            .as_mut()
            .expect("just set above")
            .write_chunk(&self.head)?;
        self.head.clear();
        Ok(())
    }

    /// This transaction's total buffered change count so far (head plus
    /// whatever has already spilled) — issue #56's `intake.commit_transaction`
    /// span reads this once, before [`Self::stage_and_advance`] consumes
    /// `self`, to record how large a commit's `changes` field was.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Clippy's `len_without_is_empty` companion — not otherwise used today,
    /// and kept only because removing it would trip that lint on `len`.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Discards everything buffered — a fresh transaction (`Begin`) starts
    /// from empty, and dropping any spill file unlinks it.
    pub fn clear(&mut self) {
        self.head.clear();
        self.spill = None;
        self.count = 0;
        self.tables.clear();
    }

    /// Runs the linchpin over this transaction's changes: spilled chunks
    /// first, in the order they were flushed, then the in-memory tail — all
    /// inside `txn`, so staging + watermark + notify stays one atomic unit
    /// even when a transaction spanned multiple chunks. `end_lsn`/`changed_at`
    /// are stamped onto every change here, not at buffering time: the
    /// decoder only learns a transaction's commit position and time from its
    /// `Commit` message, well after changes already spilled.
    ///
    /// Consumes `self` — a drained buffer is never reused, and this is also
    /// what lets the spill file be dropped (and its temp file unlinked) as
    /// soon as replay finishes.
    pub async fn stage_and_advance(
        self,
        txn: &Transaction<'_>,
        slot: &str,
        wake_channel: &str,
        end_lsn: PgLsn,
        changed_at: SystemTime,
    ) -> Result<(), IntakeError> {
        self.append_only(txn, end_lsn, changed_at).await?;
        super::advance_watermark_and_notify(txn, slot, wake_channel, end_lsn).await
    }

    /// Just the append half of [`Self::stage_and_advance`] — spilled chunks
    /// first, then the in-memory tail, both stamped with
    /// `end_lsn`/`changed_at` — with no watermark advance or notify. Issue
    /// #274 (intake group-commit): several source transactions' buffers can
    /// each [`Self::append_only`] into the *same* open `txn` in turn, each
    /// stamped with its own commit's `end_lsn`/`changed_at` (append order
    /// across transactions doesn't matter for correctness — the claim-time
    /// fold coalesces same-key rows regardless of arrival order, same
    /// argument as this module's own "Cross-chunk coalescing" doc comment
    /// note), and only the group's single
    /// [`super::advance_watermark_and_notify`] call — once, with the group's
    /// *last* transaction's `end_lsn` — needs to follow, after every buffer
    /// in the group has appended.
    pub async fn append_only(
        self,
        txn: &Transaction<'_>,
        end_lsn: PgLsn,
        changed_at: SystemTime,
    ) -> Result<(), IntakeError> {
        let replay_chunk_size = self.spill_threshold;
        if let Some(spill) = self.spill {
            let mut reader = spill.into_reader()?;
            loop {
                let mut chunk = read_chunk(&mut reader, replay_chunk_size)?;
                if chunk.is_empty() {
                    break;
                }
                super::stamp_commit_metadata(&mut chunk, end_lsn, changed_at);
                append::append(txn, &chunk).await?;
            }
        }
        let mut head = self.head;
        super::stamp_commit_metadata(&mut head, end_lsn, changed_at);
        append::append(txn, &head).await?;
        Ok(())
    }
}

/// An append-only temp file backing one transaction's spilled changes.
///
/// [`SpillFile::into_reader`] unlinks the file immediately after opening it
/// for read (POSIX semantics: an open file descriptor keeps working after
/// its directory entry is removed) — so a crash mid-replay leaves nothing on
/// disk to clean up later, and a stale file from an earlier crash can never
/// be mistaken for a live one.
struct SpillFile {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
}

impl SpillFile {
    fn create(xid: u32) -> io::Result<Self> {
        let seq = SPILL_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "trellis-intake-spill-{}-{xid}-{seq}",
            std::process::id()
        ));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            path,
            writer: Some(BufWriter::new(file)),
        })
    }

    fn write_chunk(&mut self, changes: &[StagedChange]) -> io::Result<()> {
        let w = self.writer.as_mut().expect("write after into_reader");
        for change in changes {
            write_change(w, change)?;
        }
        Ok(())
    }

    fn into_reader(mut self) -> io::Result<BufReader<File>> {
        let mut writer = self.writer.take().expect("into_reader called twice");
        writer.flush()?;
        let mut file = writer.into_inner().map_err(|e| e.into_error())?;
        file.seek(SeekFrom::Start(0))?;
        let _ = std::fs::remove_file(&self.path);
        Ok(BufReader::new(file))
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        // Reached only if a transaction spilled and then never staged (e.g.
        // the decoder wedged before Commit arrived) — `into_reader` already
        // unlinked the file on the normal path, so this is a no-op there.
        let _ = std::fs::remove_file(&self.path);
    }
}

fn read_chunk(r: &mut impl Read, max: usize) -> io::Result<Vec<StagedChange>> {
    let mut out = Vec::new();
    for _ in 0..max {
        match read_change(r)? {
            Some(change) => out.push(change),
            // A clean EOF at a record boundary — no more records, not an
            // error. Any EOF *after* the tag byte is a real error, already
            // propagated by the `?` above rather than reaching here.
            None => break,
        }
    }
    Ok(out)
}

// --- A minimal, dependency-free binary codec for StagedChange -------------
//
// No serde/bincode dependency (matching this crate's JSON-free convention in
// `staging::append`): every field is written as a fixed-width tag or a
// little-endian length prefix followed by bytes. Endianness is arbitrary —
// this format is never read by anything but the same process, in the same
// run, that wrote it.

const TAG_CDC: u8 = 0;
const TAG_RECOMPUTE: u8 = 1;
const TAG_TRUNCATE: u8 = 2;
// Issue #134: in practice this producer (intake) never emits a
// `RelationshipReverseDeferred` — only Phase 3 (`staging::apply`) does, and
// it always appends directly through an open `Transaction`, never through
// this spill buffer. The tag/codec arm exists anyway so this match stays
// exhaustive rather than needing a catch-all that would silently swallow a
// future producer's use of this variant.
const TAG_RELATIONSHIP_REVERSE_DEFERRED: u8 = 3;

fn write_str(w: &mut impl Write, s: &str) -> io::Result<()> {
    w.write_all(&(s.len() as u32).to_le_bytes())?;
    w.write_all(s.as_bytes())
}

fn read_str(r: &mut impl Read) -> io::Result<String> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn write_opt_str(w: &mut impl Write, v: Option<&str>) -> io::Result<()> {
    match v {
        None => w.write_all(&[0]),
        Some(s) => {
            w.write_all(&[1])?;
            write_str(w, s)
        }
    }
}

fn read_opt_str(r: &mut impl Read) -> io::Result<Option<String>> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag)?;
    if tag[0] == 0 {
        return Ok(None);
    }
    Ok(Some(read_str(r)?))
}

fn write_opt_str_vec(w: &mut impl Write, v: &Option<Vec<String>>) -> io::Result<()> {
    match v {
        None => w.write_all(&[0]),
        Some(items) => {
            w.write_all(&[1])?;
            w.write_all(&(items.len() as u32).to_le_bytes())?;
            for item in items {
                write_str(w, item)?;
            }
            Ok(())
        }
    }
}

fn read_opt_str_vec(r: &mut impl Read) -> io::Result<Option<Vec<String>>> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag)?;
    if tag[0] == 0 {
        return Ok(None);
    }
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut items = Vec::with_capacity(len);
    for _ in 0..len {
        items.push(read_str(r)?);
    }
    Ok(Some(items))
}

fn write_opt_u64(w: &mut impl Write, v: Option<u64>) -> io::Result<()> {
    match v {
        None => w.write_all(&[0]),
        Some(n) => {
            w.write_all(&[1])?;
            w.write_all(&n.to_le_bytes())
        }
    }
}

fn read_opt_u64(r: &mut impl Read) -> io::Result<Option<u64>> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag)?;
    if tag[0] == 0 {
        return Ok(None);
    }
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(Some(u64::from_le_bytes(buf)))
}

fn write_change(w: &mut impl Write, change: &StagedChange) -> io::Result<()> {
    match change {
        StagedChange::Cdc {
            src_table,
            key,
            op,
            lsn,
            old_image,
            new_image,
            origin_lsn,
            src_changed,
            hop_gen,
            group_key,
        } => {
            w.write_all(&[TAG_CDC])?;
            write_str(w, src_table)?;
            write_str(w, key)?;
            let op_byte = match op {
                CdcOp::Insert => 0u8,
                CdcOp::Update => 1,
                CdcOp::Delete => 2,
            };
            w.write_all(&[op_byte])?;
            write_opt_u64(w, lsn.map(u64::from))?;
            write_opt_str(w, old_image.as_deref())?;
            write_opt_str(w, new_image.as_deref())?;
            write_opt_u64(w, origin_lsn.map(u64::from))?;
            write_opt_u64(
                w,
                src_changed.map(|t| {
                    t.duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_micros() as u64
                }),
            )?;
            w.write_all(&hop_gen.to_le_bytes())?;
            write_opt_str_vec(w, group_key)
        }
        StagedChange::Recompute {
            src_table,
            key,
            hop_gen,
            group_key,
            src_changed,
            prior_image,
        } => {
            w.write_all(&[TAG_RECOMPUTE])?;
            write_str(w, src_table)?;
            write_str(w, key)?;
            w.write_all(&hop_gen.to_le_bytes())?;
            write_opt_str_vec(w, group_key)?;
            write_opt_u64(
                w,
                src_changed.map(|t| {
                    t.duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_micros() as u64
                }),
            )?;
            write_opt_str(w, prior_image.as_deref())
        }
        StagedChange::Truncate {
            src_table,
            lsn,
            origin_lsn,
            src_changed,
        } => {
            w.write_all(&[TAG_TRUNCATE])?;
            write_str(w, src_table)?;
            write_opt_u64(w, lsn.map(u64::from))?;
            write_opt_u64(w, origin_lsn.map(u64::from))?;
            write_opt_u64(
                w,
                src_changed.map(|t| {
                    t.duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_micros() as u64
                }),
            )
        }
        StagedChange::RelationshipReverseDeferred {
            src_table,
            key,
            old_image,
            new_image,
            lsn,
            src_changed,
            relationship_id,
            retry_count,
        } => {
            w.write_all(&[TAG_RELATIONSHIP_REVERSE_DEFERRED])?;
            write_str(w, src_table)?;
            write_str(w, key)?;
            write_opt_str(w, old_image.as_deref())?;
            write_opt_str(w, new_image.as_deref())?;
            write_opt_u64(w, lsn.map(u64::from))?;
            write_opt_u64(
                w,
                src_changed.map(|t| {
                    t.duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_micros() as u64
                }),
            )?;
            w.write_all(&relationship_id.to_le_bytes())?;
            w.write_all(&retry_count.to_le_bytes())
        }
    }
}

/// Reads one change, distinguishing a clean end-of-chunk from a truncated
/// record: `Ok(None)` means EOF struck exactly at a record boundary (the tag
/// byte's own read got zero bytes) — the normal, expected way a chunk ends.
/// Any EOF struck *after* the tag byte was read means a record started but
/// wasn't fully written — a truncated/corrupt spill file — and is returned
/// as a hard error rather than silently treated as "no more records," which
/// would silently drop the rest of the chunk.
///
/// The tag is read with a single `read` rather than `read_exact` so a
/// zero-byte result (true EOF) can be told apart from a partial one; every
/// other field in this format uses `read_exact` as before, and its
/// `UnexpectedEof` now propagates as a genuine error via `?` instead of
/// being caught by the caller.
fn read_change(r: &mut impl Read) -> io::Result<Option<StagedChange>> {
    let mut tag = [0u8; 1];
    if r.read(&mut tag)? == 0 {
        return Ok(None);
    }
    let change = match tag[0] {
        TAG_CDC => {
            let src_table = read_str(r)?;
            let key = read_str(r)?;
            let mut op_byte = [0u8; 1];
            r.read_exact(&mut op_byte)?;
            let op = match op_byte[0] {
                0 => CdcOp::Insert,
                1 => CdcOp::Update,
                2 => CdcOp::Delete,
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("bad spilled CdcOp byte {other}"),
                    ));
                }
            };
            let lsn = read_opt_u64(r)?.map(PgLsn::from);
            let old_image = read_opt_str(r)?;
            let new_image = read_opt_str(r)?;
            let origin_lsn = read_opt_u64(r)?.map(PgLsn::from);
            let src_changed = read_opt_u64(r)?
                .map(|micros| SystemTime::UNIX_EPOCH + Duration::from_micros(micros));
            let mut hop_gen_buf = [0u8; 4];
            r.read_exact(&mut hop_gen_buf)?;
            let hop_gen = i32::from_le_bytes(hop_gen_buf);
            let group_key = read_opt_str_vec(r)?;
            StagedChange::Cdc {
                src_table,
                key,
                op,
                lsn,
                old_image,
                new_image,
                origin_lsn,
                src_changed,
                hop_gen,
                group_key,
            }
        }
        TAG_RECOMPUTE => {
            let src_table = read_str(r)?;
            let key = read_str(r)?;
            let mut hop_gen_buf = [0u8; 4];
            r.read_exact(&mut hop_gen_buf)?;
            let hop_gen = i32::from_le_bytes(hop_gen_buf);
            let group_key = read_opt_str_vec(r)?;
            let src_changed = read_opt_u64(r)?
                .map(|micros| SystemTime::UNIX_EPOCH + Duration::from_micros(micros));
            let prior_image = read_opt_str(r)?;
            StagedChange::Recompute {
                src_table,
                key,
                hop_gen,
                group_key,
                src_changed,
                prior_image,
            }
        }
        TAG_TRUNCATE => {
            let src_table = read_str(r)?;
            let lsn = read_opt_u64(r)?.map(PgLsn::from);
            let origin_lsn = read_opt_u64(r)?.map(PgLsn::from);
            let src_changed = read_opt_u64(r)?
                .map(|micros| SystemTime::UNIX_EPOCH + Duration::from_micros(micros));
            StagedChange::Truncate {
                src_table,
                lsn,
                origin_lsn,
                src_changed,
            }
        }
        TAG_RELATIONSHIP_REVERSE_DEFERRED => {
            let src_table = read_str(r)?;
            let key = read_str(r)?;
            let old_image = read_opt_str(r)?;
            let new_image = read_opt_str(r)?;
            let lsn = read_opt_u64(r)?.map(PgLsn::from);
            let src_changed = read_opt_u64(r)?
                .map(|micros| SystemTime::UNIX_EPOCH + Duration::from_micros(micros));
            let mut relationship_id_buf = [0u8; 8];
            r.read_exact(&mut relationship_id_buf)?;
            let relationship_id = i64::from_le_bytes(relationship_id_buf);
            let mut retry_count_buf = [0u8; 4];
            r.read_exact(&mut retry_count_buf)?;
            let retry_count = i32::from_le_bytes(retry_count_buf);
            StagedChange::RelationshipReverseDeferred {
                src_table,
                key,
                old_image,
                new_image,
                lsn,
                src_changed,
                relationship_id,
                retry_count,
            }
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad spilled StagedChange tag {other}"),
            ));
        }
    };
    Ok(Some(change))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_cdc(key: &str) -> StagedChange {
        StagedChange::Cdc {
            src_table: "public.widgets".into(),
            key: key.into(),
            op: CdcOp::Update,
            lsn: None,
            old_image: Some(r#"{"id":"1"}"#.into()),
            new_image: Some(r#"{"id":"1","note":"hi"}"#.into()),
            origin_lsn: Some(PgLsn::from(42)),
            src_changed: None,
            hop_gen: 3,
            group_key: Some(vec!["g1".into(), "g2".into()]),
        }
    }

    #[test]
    fn round_trips_a_cdc_change_through_the_codec() {
        let mut buf = Vec::new();
        write_change(&mut buf, &sample_cdc("k1")).unwrap();
        let decoded = read_change(&mut &buf[..]).unwrap().expect("one record");
        match decoded {
            StagedChange::Cdc {
                key,
                old_image,
                new_image,
                origin_lsn,
                hop_gen,
                group_key,
                ..
            } => {
                assert_eq!(key, "k1");
                assert_eq!(old_image.unwrap(), r#"{"id":"1"}"#);
                assert_eq!(new_image.unwrap(), r#"{"id":"1","note":"hi"}"#);
                assert_eq!(origin_lsn, Some(PgLsn::from(42)));
                assert_eq!(hop_gen, 3);
                assert_eq!(group_key.unwrap(), vec!["g1".to_string(), "g2".to_string()]);
            }
            other => panic!("expected Cdc, got {other:?}"),
        }
    }

    #[test]
    fn round_trips_a_recompute_change() {
        let change = StagedChange::Recompute {
            src_table: "public.widgets".into(),
            key: "k2".into(),
            hop_gen: 1,
            group_key: None,
            src_changed: Some(SystemTime::UNIX_EPOCH + Duration::from_micros(123_456)),
            prior_image: Some("{\"g\": \"a\"}".into()),
        };
        let mut buf = Vec::new();
        write_change(&mut buf, &change).unwrap();
        let decoded = read_change(&mut &buf[..]).unwrap().expect("one record");
        match decoded {
            StagedChange::Recompute {
                key,
                hop_gen,
                group_key,
                src_changed,
                prior_image,
                ..
            } => {
                assert_eq!(prior_image.as_deref(), Some("{\"g\": \"a\"}"));
                assert_eq!(key, "k2");
                assert_eq!(hop_gen, 1);
                assert!(group_key.is_none());
                assert_eq!(
                    src_changed,
                    Some(SystemTime::UNIX_EPOCH + Duration::from_micros(123_456))
                );
            }
            other => panic!("expected Recompute, got {other:?}"),
        }
    }

    #[test]
    fn round_trips_a_recompute_change_with_no_origin() {
        let change = StagedChange::Recompute {
            src_table: "public.widgets".into(),
            key: "k3".into(),
            hop_gen: 0,
            group_key: None,
            src_changed: None,
            prior_image: None,
        };
        let mut buf = Vec::new();
        write_change(&mut buf, &change).unwrap();
        let decoded = read_change(&mut &buf[..]).unwrap().expect("one record");
        match decoded {
            StagedChange::Recompute { src_changed, .. } => {
                assert!(
                    src_changed.is_none(),
                    "a backfill-shaped recompute with no origin must round-trip as None"
                );
            }
            other => panic!("expected Recompute, got {other:?}"),
        }
    }

    /// Issue #134: `RelationshipReverseDeferred` round-trips through the
    /// spill codec even though, in practice, only Phase 3
    /// (`staging::apply`) ever produces this variant, always via an open
    /// `Transaction` rather than through this intake-side buffer — the
    /// codec still needs to be correct for it, not just exhaustively
    /// matched, since nothing prevents a future producer from spilling one.
    #[test]
    fn round_trips_a_relationship_reverse_deferred_change() {
        let change = StagedChange::RelationshipReverseDeferred {
            src_table: "\u{1f}trellis-rel-reverse-deferred:7".into(),
            key: "42".into(),
            old_image: Some(r#"{"id":42,"name":"old"}"#.into()),
            new_image: Some(r#"{"id":42,"name":"new"}"#.into()),
            lsn: Some(PgLsn::from(500)),
            src_changed: Some(SystemTime::UNIX_EPOCH + Duration::from_micros(9_000)),
            relationship_id: 7,
            retry_count: 3,
        };
        let mut buf = Vec::new();
        write_change(&mut buf, &change).unwrap();
        let decoded = read_change(&mut &buf[..]).unwrap().expect("one record");
        match decoded {
            StagedChange::RelationshipReverseDeferred {
                src_table,
                key,
                old_image,
                new_image,
                lsn,
                src_changed,
                relationship_id,
                retry_count,
            } => {
                assert_eq!(src_table, "\u{1f}trellis-rel-reverse-deferred:7");
                assert_eq!(key, "42");
                assert_eq!(old_image.unwrap(), r#"{"id":42,"name":"old"}"#);
                assert_eq!(new_image.unwrap(), r#"{"id":42,"name":"new"}"#);
                assert_eq!(lsn, Some(PgLsn::from(500)));
                assert_eq!(
                    src_changed,
                    Some(SystemTime::UNIX_EPOCH + Duration::from_micros(9_000))
                );
                assert_eq!(relationship_id, 7);
                assert_eq!(retry_count, 3);
            }
            other => panic!("expected RelationshipReverseDeferred, got {other:?}"),
        }
    }

    #[test]
    fn read_chunk_stops_cleanly_at_eof() {
        let mut buf = Vec::new();
        write_change(&mut buf, &sample_cdc("a")).unwrap();
        write_change(&mut buf, &sample_cdc("b")).unwrap();
        let chunk = read_chunk(&mut &buf[..], 10).unwrap();
        assert_eq!(chunk.len(), 2);
    }

    #[test]
    fn read_chunk_errors_on_a_mid_record_truncation_instead_of_silently_stopping() {
        let mut buf = Vec::new();
        write_change(&mut buf, &sample_cdc("a")).unwrap();
        // A second record whose tag byte was written, but whose src_table
        // length prefix (4 bytes) was cut off after just one — standing in
        // for a spill file truncated mid-write. This EOF strikes *after*
        // the tag byte, so it must be a hard error, not a clean chunk end.
        buf.push(TAG_CDC);
        buf.push(0);
        let err = read_chunk(&mut &buf[..], 10).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn push_spills_past_the_threshold_and_enforces_the_hard_cap() {
        let mut buffer = TxnBuffer::new(2, 3);
        buffer.push(sample_cdc("a"), 100).unwrap();
        buffer.push(sample_cdc("b"), 100).unwrap();
        // The threshold (2) was just hit, so this push should have spilled
        // the head — nothing observable from here, but the third push below
        // must still be counted against the hard cap.
        buffer.push(sample_cdc("c"), 100).unwrap();
        let err = buffer.push(sample_cdc("d"), 100).unwrap_err();
        match err {
            IntakeError::TransactionTooLarge { xid, cap, tables } => {
                assert_eq!(xid, 100);
                assert_eq!(cap, 3);
                assert_eq!(tables, vec!["public.widgets".to_string()]);
            }
            other => panic!("expected TransactionTooLarge, got {other:?}"),
        }
    }
}
