//! A hand-rolled decoder for `pgoutput` protocol-v1 messages (issue #7).
//! Pure bytes in, typed messages out: nothing here touches a connection, a
//! `Transaction`, or async, so wire-format correctness can be tested with
//! byte fixtures alone.
//!
//! `Begin`/`Commit` boundary messages are the exception: the transport
//! ([`pgwire_replication::ReplicationClient`]) peels those off for its own
//! `xid`/`final_lsn` bookkeeping and only hands us every *other* message via
//! `ReplicationEvent::XLogData`. The variants still exist and are unit-tested
//! so this stays a complete decoder, but the production path never feeds
//! their bytes to [`decode`].
//!
//! Message shapes are `pgoutput` protocol version 1, documented at
//! <https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>.

use std::collections::HashMap;
use std::fmt;

/// A decode failure: bytes that didn't match `pgoutput` protocol v1, or a
/// relation named before its `Relation` message arrived. The design doc
/// calls both unreachable from a well-formed server — this exists to *name*
/// the wedge distinctly from an I/O error, not to make it recoverable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Ran out of bytes mid-message.
    Truncated {
        message: &'static str,
        field: &'static str,
    },
    /// The leading message-type byte wasn't one this decoder recognizes.
    UnknownMessageType(u8),
    /// A column's kind byte wasn't `'n'`, `'u'`, `'t'`, or `'b'`.
    UnknownColumnKind(u8),
    /// A tuple *marker* byte — the `'K'`/`'O'`/`'N'` that introduces a
    /// TupleData section in an `Update`/`Delete` — wasn't one this decoder
    /// recognizes. Distinct from [`DecodeError::UnknownColumnKind`], which is
    /// about a byte *inside* a tuple, not the marker that precedes it.
    UnknownTupleMarker { message: &'static str, marker: u8 },
    /// An `Insert`/`Update`/`Delete` named a `relation_id` with no prior
    /// `Relation` message describing it.
    UnknownRelation(u32),
    /// Text bytes weren't valid UTF-8. Text mode (the only mode this decoder
    /// supports) guarantees this in practice, but the conversion is fallible,
    /// so it's surfaced rather than `unwrap`ped.
    InvalidUtf8 { field: &'static str },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Truncated { message, field } => {
                write!(f, "truncated pgoutput {message} message reading {field}")
            }
            DecodeError::UnknownMessageType(b) => {
                write!(f, "unknown pgoutput message type byte {b:#x} ({b})")
            }
            DecodeError::UnknownColumnKind(b) => {
                write!(f, "unknown pgoutput tuple column kind byte {b:#x} ({b})")
            }
            DecodeError::UnknownTupleMarker { message, marker } => {
                write!(
                    f,
                    "unknown pgoutput {message} tuple marker byte {marker:#x} ({marker})"
                )
            }
            DecodeError::UnknownRelation(id) => {
                write!(f, "no Relation message seen yet for relation id {id}")
            }
            DecodeError::InvalidUtf8 { field } => {
                write!(f, "pgoutput {field} was not valid UTF-8")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// One column's value in a decoded tuple.
///
/// [`ColumnValue::Unchanged`] only appears in an `Update`'s *old* tuple, for
/// a TOASTed column Postgres didn't re-send because it didn't change — the
/// classic logical-decoding footgun. Intake omits the column rather than
/// guessing; the apply half re-reads current source state if it needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnValue {
    Null,
    Unchanged,
    Text(String),
}

/// One column in a `Relation` message's tuple descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    pub type_oid: i32,
    pub type_modifier: i32,
    /// Whether this column is part of the row's replica identity (roughly
    /// the primary key) — the flag that lets a client without the full old
    /// image still tell which row an `Update`/`Delete` targets.
    pub is_key: bool,
}

/// A decoded `Relation` message: the tuple shape for one source table,
/// cached by its PostgreSQL `pg_class.oid` (`relation_id`) so later
/// `Insert`/`Update`/`Delete` messages (which carry only that OID, not the
/// shape) can be resolved against it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    /// PostgreSQL's physical relation OID (`pg_class.oid`), not a local cache
    /// identifier or relation-name surrogate.
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
    /// The table's replica identity as reported by the `Relation` message:
    /// `'d'` (default), `'n'` (nothing), `'f'` (full), `'i'` (index). Kept
    /// raw — nothing here acts on it; #7's REPLICA IDENTITY FULL requirement
    /// (see `intake::replica_identity`) is a definition-time check, not a
    /// decode-time one.
    pub replica_identity: u8,
    pub columns: Vec<ColumnInfo>,
}

/// A decoded `pgoutput` message. `Begin`/`Commit` are included for
/// completeness and unit-tested, but the production path never feeds their
/// bytes through [`decode`] (see the module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Begin {
        final_lsn: u64,
        commit_timestamp: i64,
        xid: u32,
    },
    Commit {
        commit_lsn: u64,
        end_lsn: u64,
        commit_timestamp: i64,
    },
    /// A transactional origin tag. Intake has no use for it — decoded so an
    /// origin-tagged stream doesn't wedge the decoder, otherwise a no-op.
    Origin {
        origin_lsn: u64,
        name: String,
    },
    Relation(Relation),
    /// A custom Postgres type used by a replicated column. Intake treats
    /// every value as text regardless of type, so this is decoded only so it
    /// doesn't wedge the stream, otherwise a no-op.
    Type {
        type_id: i32,
        namespace: String,
        name: String,
    },
    Insert {
        relation_id: u32,
        new: Vec<ColumnValue>,
    },
    Update {
        relation_id: u32,
        /// Present iff the server sent an old-row image: a 'K' (key-only) or
        /// 'O' (full, `REPLICA IDENTITY FULL`) section preceded the new
        /// tuple. The `bool` is `true` for key-only. Either way the tuple is
        /// a full-width positional vector — a key-only image sends every
        /// column with the non-key ones null ('n'), so key columns sit at
        /// their real positions and [`named_columns`] lines them up.
        old: Option<(bool, Vec<ColumnValue>)>,
        new: Vec<ColumnValue>,
    },
    Delete {
        relation_id: u32,
        /// See [`Message::Update::old`]'s second field.
        key_only: bool,
        old: Vec<ColumnValue>,
    },
    /// A `TRUNCATE` of one or more replicated tables. Intake stages one
    /// sentinel per PostgreSQL relation OID after resolving it in the
    /// relation cache.
    Truncate {
        /// `TRUNCATE` option bits (bit 0 = `CASCADE`, bit 1 = `RESTART
        /// IDENTITY`), kept raw.
        options: u8,
        /// The PostgreSQL relation OIDs named by this `TRUNCATE`, each resolvable
        /// against the [`RelationCache`] the same way DML `relation_id`s are.
        relation_ids: Vec<u32>,
    },
}

/// A cursor over a message's bytes. Every `pgoutput` integer is big-endian,
/// so this is the one place that matters; call sites just ask for the next
/// N-byte int.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(
        &mut self,
        n: usize,
        message: &'static str,
        field: &'static str,
    ) -> Result<&'a [u8], DecodeError> {
        if self.buf.len() - self.pos < n {
            return Err(DecodeError::Truncated { message, field });
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn u8(&mut self, message: &'static str, field: &'static str) -> Result<u8, DecodeError> {
        Ok(self.take(1, message, field)?[0])
    }

    fn i16(&mut self, message: &'static str, field: &'static str) -> Result<i16, DecodeError> {
        let b = self.take(2, message, field)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    fn i32(&mut self, message: &'static str, field: &'static str) -> Result<i32, DecodeError> {
        let b = self.take(4, message, field)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u32(&mut self, message: &'static str, field: &'static str) -> Result<u32, DecodeError> {
        let b = self.take(4, message, field)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i64(&mut self, message: &'static str, field: &'static str) -> Result<i64, DecodeError> {
        let b = self.take(8, message, field)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn u64(&mut self, message: &'static str, field: &'static str) -> Result<u64, DecodeError> {
        Ok(self.i64(message, field)? as u64)
    }

    /// A NUL-terminated string (`pgoutput`'s `cstring` fields: table/column
    /// names, the origin name).
    fn cstring(
        &mut self,
        message: &'static str,
        field: &'static str,
    ) -> Result<String, DecodeError> {
        let start = self.pos;
        loop {
            let b = self.take(1, message, field)?[0];
            if b == 0 {
                break;
            }
        }
        let bytes = &self.buf[start..self.pos - 1];
        String::from_utf8(bytes.to_vec()).map_err(|_| DecodeError::InvalidUtf8 { field })
    }

    /// One `pgoutput` "TupleData" section: an int16 column count, then per
    /// column a kind byte and (for `'t'`/`'b'`) a length-prefixed payload.
    fn tuple(&mut self, message: &'static str) -> Result<Vec<ColumnValue>, DecodeError> {
        let n = self.i16(message, "tuple column count")?;
        let mut values = Vec::with_capacity(n.max(0) as usize);
        for _ in 0..n {
            let kind = self.u8(message, "tuple column kind")?;
            let value = match kind {
                b'n' => ColumnValue::Null,
                b'u' => ColumnValue::Unchanged,
                b't' => {
                    let len = self.i32(message, "tuple column text length")? as usize;
                    let bytes = self.take(len, message, "tuple column text")?;
                    let text = String::from_utf8(bytes.to_vec()).map_err(|_| {
                        DecodeError::InvalidUtf8 {
                            field: "tuple column text",
                        }
                    })?;
                    ColumnValue::Text(text)
                }
                // Binary format ('b'): intake never requests it, so a
                // well-formed server never sends it. Still decoded (it's
                // length-prefixed) as a hex string so an unexpected binary
                // column doesn't wedge the stream, though nothing interprets
                // it.
                b'b' => {
                    let len = self.i32(message, "tuple column binary length")? as usize;
                    let bytes = self.take(len, message, "tuple column binary")?;
                    ColumnValue::Text(format!("\\x{}", hex_encode(bytes)))
                }
                other => return Err(DecodeError::UnknownColumnKind(other)),
            };
            values.push(value);
        }
        Ok(values)
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Decodes one `pgoutput` message from `bytes` — the payload of a
/// `ReplicationEvent::XLogData`, with the leading message-type byte still
/// present.
pub fn decode(bytes: &[u8]) -> Result<Message, DecodeError> {
    let mut cur = Cursor::new(bytes);
    let tag = cur.u8("message", "type tag")?;
    match tag {
        b'B' => {
            let final_lsn = cur.u64("Begin", "final_lsn")?;
            let commit_timestamp = cur.i64("Begin", "commit_timestamp")?;
            let xid = cur.u32("Begin", "xid")?;
            Ok(Message::Begin {
                final_lsn,
                commit_timestamp,
                xid,
            })
        }
        b'C' => {
            let _flags = cur.u8("Commit", "flags")?; // reserved, always 0
            let commit_lsn = cur.u64("Commit", "commit_lsn")?;
            let end_lsn = cur.u64("Commit", "end_lsn")?;
            let commit_timestamp = cur.i64("Commit", "commit_timestamp")?;
            Ok(Message::Commit {
                commit_lsn,
                end_lsn,
                commit_timestamp,
            })
        }
        b'O' => {
            let origin_lsn = cur.u64("Origin", "origin_lsn")?;
            let name = cur.cstring("Origin", "origin_name")?;
            Ok(Message::Origin { origin_lsn, name })
        }
        b'R' => {
            let relation_id = cur.u32("Relation", "relation_id")?;
            let namespace = cur.cstring("Relation", "namespace")?;
            let name = cur.cstring("Relation", "relation_name")?;
            let replica_identity = cur.u8("Relation", "replica_identity")?;
            let num_columns = cur.i16("Relation", "column count")?;
            let mut columns = Vec::with_capacity(num_columns.max(0) as usize);
            for _ in 0..num_columns {
                let flags = cur.u8("Relation", "column flags")?;
                let col_name = cur.cstring("Relation", "column name")?;
                let type_oid = cur.i32("Relation", "column type oid")?;
                let type_modifier = cur.i32("Relation", "column type modifier")?;
                columns.push(ColumnInfo {
                    name: col_name,
                    type_oid,
                    type_modifier,
                    is_key: flags & 1 == 1,
                });
            }
            Ok(Message::Relation(Relation {
                relation_id,
                namespace,
                name,
                replica_identity,
                columns,
            }))
        }
        b'Y' => {
            let type_id = cur.i32("Type", "data type oid")?;
            let namespace = cur.cstring("Type", "namespace")?;
            let name = cur.cstring("Type", "type name")?;
            Ok(Message::Type {
                type_id,
                namespace,
                name,
            })
        }
        b'I' => {
            let relation_id = cur.u32("Insert", "relation_id")?;
            let marker = cur.u8("Insert", "new tuple marker")?;
            if marker != b'N' {
                return Err(DecodeError::UnknownTupleMarker {
                    message: "Insert",
                    marker,
                });
            }
            let new = cur.tuple("Insert")?;
            Ok(Message::Insert { relation_id, new })
        }
        b'U' => {
            let relation_id = cur.u32("Update", "relation_id")?;
            let mut marker = cur.u8("Update", "tuple marker")?;
            let old = if marker == b'K' || marker == b'O' {
                let key_only = marker == b'K';
                let old_tuple = cur.tuple("Update")?;
                marker = cur.u8("Update", "new tuple marker")?;
                Some((key_only, old_tuple))
            } else {
                None
            };
            if marker != b'N' {
                return Err(DecodeError::UnknownTupleMarker {
                    message: "Update",
                    marker,
                });
            }
            let new = cur.tuple("Update")?;
            Ok(Message::Update {
                relation_id,
                old,
                new,
            })
        }
        b'D' => {
            let relation_id = cur.u32("Delete", "relation_id")?;
            let marker = cur.u8("Delete", "tuple marker")?;
            let key_only = match marker {
                b'K' => true,
                b'O' => false,
                other => {
                    return Err(DecodeError::UnknownTupleMarker {
                        message: "Delete",
                        marker: other,
                    });
                }
            };
            let old = cur.tuple("Delete")?;
            Ok(Message::Delete {
                relation_id,
                key_only,
                old,
            })
        }
        b'T' => {
            // TRUNCATE: Int32 relation count, Int8 option bits, then that
            // many Int32 relation ids.
            let num_relations = cur.i32("Truncate", "relation count")?;
            let options = cur.u8("Truncate", "options")?;
            let mut relation_ids = Vec::with_capacity(num_relations.max(0) as usize);
            for _ in 0..num_relations {
                relation_ids.push(cur.u32("Truncate", "relation_id")?);
            }
            Ok(Message::Truncate {
                options,
                relation_ids,
            })
        }
        other => Err(DecodeError::UnknownMessageType(other)),
    }
}

/// The relation cache a stream of `Insert`/`Update`/`Delete` messages must
/// be resolved against: `pgoutput` sends a `Relation` message once (then
/// again only if the shape changes) and every subsequent DML message for
/// that table names only its PostgreSQL relation OID (`relation_id`).
#[derive(Debug, Default)]
pub struct RelationCache {
    by_id: HashMap<u32, Relation>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, relation: Relation) {
        self.by_id.insert(relation.relation_id, relation.clone());
    }

    pub fn get(&self, relation_id: u32) -> Result<&Relation, DecodeError> {
        self.by_id
            .get(&relation_id)
            .ok_or(DecodeError::UnknownRelation(relation_id))
    }
}

/// Zips a tuple's positional [`ColumnValue`]s with the [`Relation`]'s column
/// names, in order — `pgoutput` tuples carry no names of their own, only
/// position, so this is the one place that mapping happens.
pub fn named_columns<'a>(
    relation: &'a Relation,
    tuple: &'a [ColumnValue],
) -> impl Iterator<Item = (&'a str, &'a ColumnValue)> {
    relation
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .zip(tuple.iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds the bytes of a `Relation` message for a two-column table
    /// `public.widgets(id, name)`, id as the (only) key column — the
    /// fixture most of the DML tests below decode against.
    fn widgets_relation_bytes() -> Vec<u8> {
        let mut b = vec![b'R'];
        b.extend_from_slice(&7u32.to_be_bytes()); // relation_id (pg_class.oid)
        b.extend_from_slice(b"public\0");
        b.extend_from_slice(b"widgets\0");
        b.push(b'd'); // replica identity: default
        b.extend_from_slice(&2i16.to_be_bytes()); // 2 columns
        // id: key, oid 23 (int4), no modifier
        b.push(1);
        b.extend_from_slice(b"id\0");
        b.extend_from_slice(&23i32.to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());
        // name: not key, oid 25 (text)
        b.push(0);
        b.extend_from_slice(b"name\0");
        b.extend_from_slice(&25i32.to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());
        b
    }

    fn widgets_relation() -> Relation {
        match decode(&widgets_relation_bytes()).unwrap() {
            Message::Relation(r) => r,
            other => panic!("expected Relation, got {other:?}"),
        }
    }

    #[test]
    fn decodes_relation_message() {
        let r = widgets_relation();
        assert_eq!(r.relation_id, 7);
        assert_eq!(r.namespace, "public");
        assert_eq!(r.name, "widgets");
        assert_eq!(r.replica_identity, b'd');
        assert_eq!(r.columns.len(), 2);
        assert_eq!(r.columns[0].name, "id");
        assert!(r.columns[0].is_key);
        assert_eq!(r.columns[1].name, "name");
        assert!(!r.columns[1].is_key);
    }

    #[test]
    fn decodes_a_relation_oid_above_i32_max() {
        let mut bytes = widgets_relation_bytes();
        bytes[1..5].copy_from_slice(&0xf000_0000u32.to_be_bytes());
        match decode(&bytes).unwrap() {
            Message::Relation(relation) => assert_eq!(relation.relation_id, 0xf000_0000),
            other => panic!("expected Relation, got {other:?}"),
        }
    }

    #[test]
    fn decodes_begin_message() {
        let mut b = vec![b'B'];
        b.extend_from_slice(&0x16_B374_D848u64.to_be_bytes());
        b.extend_from_slice(&123_456_789i64.to_be_bytes());
        b.extend_from_slice(&42u32.to_be_bytes());
        match decode(&b).unwrap() {
            Message::Begin {
                final_lsn,
                commit_timestamp,
                xid,
            } => {
                assert_eq!(final_lsn, 0x16_B374_D848);
                assert_eq!(commit_timestamp, 123_456_789);
                assert_eq!(xid, 42);
            }
            other => panic!("expected Begin, got {other:?}"),
        }
    }

    #[test]
    fn decodes_commit_message() {
        let mut b = vec![b'C'];
        b.push(0); // flags, reserved
        b.extend_from_slice(&100u64.to_be_bytes()); // commit_lsn
        b.extend_from_slice(&200u64.to_be_bytes()); // end_lsn
        b.extend_from_slice(&555i64.to_be_bytes());
        match decode(&b).unwrap() {
            Message::Commit {
                commit_lsn,
                end_lsn,
                commit_timestamp,
            } => {
                assert_eq!(commit_lsn, 100);
                assert_eq!(end_lsn, 200);
                assert_eq!(commit_timestamp, 555);
            }
            other => panic!("expected Commit, got {other:?}"),
        }
    }

    #[test]
    fn decodes_origin_message_as_a_no_op_payload() {
        let mut b = vec![b'O'];
        b.extend_from_slice(&9u64.to_be_bytes());
        b.extend_from_slice(b"upstream\0");
        match decode(&b).unwrap() {
            Message::Origin { origin_lsn, name } => {
                assert_eq!(origin_lsn, 9);
                assert_eq!(name, "upstream");
            }
            other => panic!("expected Origin, got {other:?}"),
        }
    }

    #[test]
    fn decodes_type_message_as_a_no_op_payload() {
        let mut b = vec![b'Y'];
        b.extend_from_slice(&16400i32.to_be_bytes());
        b.extend_from_slice(b"public\0");
        b.extend_from_slice(b"widget_status\0");
        match decode(&b).unwrap() {
            Message::Type {
                type_id,
                namespace,
                name,
            } => {
                assert_eq!(type_id, 16400);
                assert_eq!(namespace, "public");
                assert_eq!(name, "widget_status");
            }
            other => panic!("expected Type, got {other:?}"),
        }
    }

    #[test]
    fn decodes_insert_message_with_named_columns() {
        let relation = widgets_relation();
        let mut b = vec![b'I'];
        b.extend_from_slice(&7i32.to_be_bytes());
        b.push(b'N');
        b.extend_from_slice(&2i16.to_be_bytes());
        b.push(b't');
        b.extend_from_slice(&1i32.to_be_bytes());
        b.extend_from_slice(b"1");
        b.push(b't');
        b.extend_from_slice(&6i32.to_be_bytes());
        b.extend_from_slice(b"widget");

        match decode(&b).unwrap() {
            Message::Insert { relation_id, new } => {
                assert_eq!(relation_id, 7);
                let named: Vec<_> = named_columns(&relation, &new).collect();
                assert_eq!(named[0], ("id", &ColumnValue::Text("1".into())));
                assert_eq!(named[1], ("name", &ColumnValue::Text("widget".into())));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn decodes_update_with_key_only_old_image() {
        let mut b = vec![b'U'];
        b.extend_from_slice(&7i32.to_be_bytes());
        // old: 'K' (key-only), one column (id=1)
        b.push(b'K');
        b.extend_from_slice(&1i16.to_be_bytes());
        b.push(b't');
        b.extend_from_slice(&1i32.to_be_bytes());
        b.extend_from_slice(b"1");
        // new: full row
        b.push(b'N');
        b.extend_from_slice(&2i16.to_be_bytes());
        b.push(b't');
        b.extend_from_slice(&1i32.to_be_bytes());
        b.extend_from_slice(b"1");
        b.push(b't');
        b.extend_from_slice(&7i32.to_be_bytes());
        b.extend_from_slice(b"renamed");

        match decode(&b).unwrap() {
            Message::Update {
                relation_id,
                old,
                new,
            } => {
                assert_eq!(relation_id, 7);
                let (key_only, old_tuple) = old.expect("expected an old image");
                assert!(key_only);
                assert_eq!(old_tuple, vec![ColumnValue::Text("1".into())]);
                assert_eq!(new.len(), 2);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn decodes_update_with_no_old_image() {
        let mut b = vec![b'U'];
        b.extend_from_slice(&7i32.to_be_bytes());
        b.push(b'N');
        b.extend_from_slice(&1i16.to_be_bytes());
        b.push(b'n'); // null
        match decode(&b).unwrap() {
            Message::Update { old, new, .. } => {
                assert!(old.is_none());
                assert_eq!(new, vec![ColumnValue::Null]);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn decodes_delete_with_full_old_image() {
        let mut b = vec![b'D'];
        b.extend_from_slice(&7i32.to_be_bytes());
        b.push(b'O'); // full old row (REPLICA IDENTITY FULL)
        b.extend_from_slice(&2i16.to_be_bytes());
        b.push(b't');
        b.extend_from_slice(&1i32.to_be_bytes());
        b.extend_from_slice(b"1");
        b.push(b'u'); // unchanged TOASTed column
        match decode(&b).unwrap() {
            Message::Delete {
                relation_id,
                key_only,
                old,
            } => {
                assert_eq!(relation_id, 7);
                assert!(!key_only);
                assert_eq!(
                    old,
                    vec![ColumnValue::Text("1".into()), ColumnValue::Unchanged]
                );
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn decodes_truncate_message() {
        let mut b = vec![b'T'];
        b.extend_from_slice(&2i32.to_be_bytes()); // 2 relations
        b.push(0b01); // CASCADE
        b.extend_from_slice(&7i32.to_be_bytes());
        b.extend_from_slice(&8i32.to_be_bytes());
        match decode(&b).unwrap() {
            Message::Truncate {
                options,
                relation_ids,
            } => {
                assert_eq!(options, 0b01);
                assert_eq!(relation_ids, vec![7, 8]);
            }
            other => panic!("expected Truncate, got {other:?}"),
        }
    }

    #[test]
    fn rejects_a_bad_insert_tuple_marker() {
        // A malformed marker must produce a clean typed error, not silently
        // misparse the following bytes.
        let mut b = vec![b'I'];
        b.extend_from_slice(&7i32.to_be_bytes());
        b.push(b'X'); // not 'N'
        match decode(&b) {
            Err(DecodeError::UnknownTupleMarker {
                message: "Insert",
                marker: b'X',
            }) => {}
            other => panic!("expected UnknownTupleMarker, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_message_type() {
        match decode(b"?") {
            Err(DecodeError::UnknownMessageType(b'?')) => {}
            other => panic!("expected UnknownMessageType, got {other:?}"),
        }
    }

    #[test]
    fn rejects_truncated_message() {
        // A Begin message missing its xid.
        let mut b = vec![b'B'];
        b.extend_from_slice(&1u64.to_be_bytes());
        b.extend_from_slice(&1i64.to_be_bytes());
        match decode(&b) {
            Err(DecodeError::Truncated {
                message: "Begin",
                field: "xid",
            }) => {}
            other => panic!("expected Truncated, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_tuple_column_kind() {
        let mut b = vec![b'I'];
        b.extend_from_slice(&7i32.to_be_bytes());
        b.push(b'N');
        b.extend_from_slice(&1i16.to_be_bytes());
        b.push(b'?');
        match decode(&b) {
            Err(DecodeError::UnknownColumnKind(b'?')) => {}
            other => panic!("expected UnknownColumnKind, got {other:?}"),
        }
    }

    #[test]
    fn relation_cache_resolves_dml_against_a_prior_relation_message() {
        let mut cache = RelationCache::new();
        assert!(matches!(cache.get(7), Err(DecodeError::UnknownRelation(7))));
        cache.record(widgets_relation());
        assert_eq!(cache.get(7).unwrap().name, "widgets");
    }
}
