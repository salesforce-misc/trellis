# frozen_string_literal: true

module Trellis
  # Times cross the native boundary as signed microseconds since the Unix
  # epoch (ADR-0010 decision 4) and become Times here, in Ruby, not in Rust.
  # Not part of the public API.
  module EpochMicros
    # The largest and smallest epoch microseconds the engine takes (an i64).
    RANGE = (-(2**63))..((2**63) - 1)

    def self.to_time(micros)
      ::Time.at(0, micros, :microsecond, in: "UTC")
    end

    # `time` in whole microseconds, truncated toward the past, or nil if the
    # engine can't represent it.
    def self.from_time(time)
      micros = (time.to_r * 1_000_000).floor
      micros if RANGE.cover?(micros)
    end
  end

  # A registered transform definition, as Trellis.define returns it (and
  # Trellis.apply, inside an Applied).
  #
  # - id: the definition's id.
  # - target_table, source_table: fully qualified, "schema.table".
  # - source_version: the source table's version it was validated against.
  # - status: a symbol (:waiting_to_backfill, :backfilling, :live, ...), from
  #   a closed set: never one made from a string the database returned.
  # - source_columns: each source column's name => its type's name
  #   ("integer", "numeric", "text", ...).
  Definition = Data.define(:id, :target_table, :source_table, :source_version, :status,
                           :source_columns)

  # A registered transform definition, as Trellis.definitions lists it:
  # Definition's fields, with the time it was registered (a Time) in place
  # of its source columns. backfill_failure is nil unless its build keeps
  # failing. halt is nil unless the drain halted on it: a CaptureFailure of
  # kind :halt, the definition :paused until the cause is fixed and it is
  # resumed. Listing the definitions and looking for a halt is the health
  # check for halts.
  DefinitionSummary = Data.define(:id, :target_table, :source_table, :source_version, :status,
                                  :created_at, :backfill_failure, :halt) do
    def self.from_native(hash)
      failure = hash[:backfill_failure]
      halt = hash[:halt]
      new(**hash.except(:created_at_micros, :backfill_failure, :halt),
          created_at: EpochMicros.to_time(hash.fetch(:created_at_micros)),
          backfill_failure: failure && BackfillFailure.from_native(failure),
          halt: halt && CaptureFailure.from_native(halt))
    end
  end

  # A definition's status, as Trellis.status returns it.
  #
  # status is one of :waiting_to_backfill, :backfilling, :catching_up,
  # :live, :quarantined or :paused. A newly defined transform starts
  # :waiting_to_backfill and reaches :live once some process in the fleet
  # runs the staging worker and drain threads (see Trellis.connect).
  #
  # The other fields say what the definition is stuck on, and are nil when
  # nothing holds it up:
  # - backfill_failure: its build keeps failing (one of its build chunks, or
  #   its source table's backfill).
  # - capture_wait: installing or widening capture on a table it reads waits
  #   for a lock another session holds. It clears once that session lets go.
  # - capture_failure: capture of a table it reads is broken, or the drain
  #   halted on it. A schema change paused it (resume it once fixed),
  #   installing capture keeps failing (it clears once the cause is fixed),
  #   or, with kind :halt, a failure no retry gets past reached it, so the
  #   drain paused it and what depends on it (resume it once fixed). A
  #   definition an operator paused has none.
  # Every process sees them, whichever one runs the staging worker.
  Status = Data.define(:status, :backfill_failure, :capture_wait, :capture_failure) do
    def self.from_native(hash)
      failure = hash[:backfill_failure]
      wait = hash[:capture_wait]
      capture_failure = hash[:capture_failure]
      new(status: hash[:status],
          backfill_failure: failure && BackfillFailure.from_native(failure),
          capture_wait: wait && CaptureWait.from_native(wait),
          capture_failure: capture_failure && CaptureFailure.from_native(capture_failure))
    end
  end

  # What a definition's capture waits on: the staging worker's install,
  # widen or uninstall (operation, a String) of the capture triggers on
  # table needs lock_mode, and another session holds or is queued for a
  # conflicting lock. blockers has one line per such session. waiting_since
  # and observed_at are Times.
  CaptureWait = Data.define(:table, :operation, :lock_mode, :waiting_since, :observed_at,
                            :blockers) do
    def self.from_native(hash)
      new(**hash.except(:waiting_since_micros, :observed_at_micros),
          waiting_since: EpochMicros.to_time(hash.fetch(:waiting_since_micros)),
          observed_at: EpochMicros.to_time(hash.fetch(:observed_at_micros)))
    end
  end

  # Why capture of source_table is broken, or why the drain halted on the
  # definition: kind is :capture or :halt, error is a sentence naming the
  # cause, columns the columns it is about (empty when it isn't about a
  # column, and for a halt), and detected_at (a Time) when it was first
  # found.
  CaptureFailure = Data.define(:kind, :source_table, :columns, :error, :detected_at) do
    def self.from_native(hash)
      new(**hash.except(:detected_at_micros),
          detected_at: EpochMicros.to_time(hash.fetch(:detected_at_micros)))
    end
  end

  # Why a definition's build isn't finishing: the build of source_table has
  # failed `attempts` times, the latest being last_error, and won't be
  # retried before next_attempt_at (a Time). It retries on its own, except
  # that a build failing for a reason no row explains pauses the definition
  # after a few attempts (next_attempt_at is then when it paused). Fix the
  # cause, resume a paused definition, and the next attempt goes through.
  BackfillFailure = Data.define(:source_table, :attempts, :last_error, :next_attempt_at) do
    def self.from_native(hash)
      new(**hash.except(:next_attempt_at_micros),
          next_attempt_at: EpochMicros.to_time(hash.fetch(:next_attempt_at_micros)))
    end
  end

  # A relationship a RELATIONSHIP statement registered, as Trellis.apply
  # reports it (inside an Applied).
  #
  # cardinality is :one when to_col is the sole column of a primary key or
  # unique index on to_table (at most one related row), and :many otherwise.
  # warnings holds each non-fatal caveat the declaration raised, such as a
  # missing index on from_col, as its message.
  Relationship = Data.define(:id, :name, :from_schema, :from_table, :from_col, :to_schema,
                             :to_table, :to_col, :cardinality, :warnings)

  # A registered relationship, as Trellis.relationships lists it:
  # Relationship's fields, with the time it was declared (a Time) in place of
  # its creation-time warnings.
  RelationshipSummary = Data.define(:id, :name, :from_schema, :from_table, :from_col,
                                    :to_schema, :to_table, :to_col, :cardinality,
                                    :created_at) do
    def self.from_native(hash)
      new(**hash.except(:created_at_micros),
          created_at: EpochMicros.to_time(hash.fetch(:created_at_micros)))
    end
  end

  # What Trellis.apply did. `kind` says which statement form ran, and each
  # other field is set only for the kinds that carry it (nil otherwise):
  #
  # - :transform_defined: a TRANSFORM statement registered `definition` (a
  #   Definition), at :waiting_to_backfill.
  # - :relationship_defined: a RELATIONSHIP statement registered
  #   `relationship` (a Relationship).
  # - :paused: a PAUSE TRANSFORM statement froze its subject, or found it
  #   already frozen.
  # - :resumed: a RESUME TRANSFORM statement unfroze its subject. `columns`
  #   lists every column a column resume resumed, as "transform.column": the
  #   one it named, then any dependent whose pause was only that one's
  #   cascade. It is [] for a whole-transform resume, which drops the
  #   transform to :waiting_to_backfill to rebuild.
  # - :dropped: a DROP statement removed its subject, or found it already
  #   gone.
  # - :altered: an ALTER TRANSFORM statement edited `definition`. `added`,
  #   `dropped` and `altered` name only the fields this call changed.
  # - :unknown: the statement was applied, but its outcome is newer than this
  #   version of the binding, which has no shape for it. It is still a
  #   success: don't retry the statement, which would apply it twice.
  #
  # It pattern-matches by kind:
  #
  #   case Trellis.apply("RESUME TRANSFORM order_totals.total")
  #   in { kind: :resumed, columns: } then puts "resumed #{columns.join(', ')}"
  #   end
  Applied = Data.define(:kind, :definition, :relationship, :columns, :added, :dropped,
                        :altered) do
    def self.from_native(hash)
      definition = hash[:definition]
      relationship = hash[:relationship]
      new(**hash,
          definition: definition && Definition.new(**definition),
          relationship: relationship && Relationship.new(**relationship))
    end
  end

  # One target's quarantine state, as Trellis.quarantined lists it and
  # Trellis.quarantine_status reports it.
  #
  # target is an address: a transform's bare target table name
  # ("order_totals") for the whole transform, or "order_totals.total" for one
  # of its columns. Pass it straight back to Trellis.quarantine_status,
  # Trellis.sample_quarantined, or a RESUME TRANSFORM statement.
  #
  # state is :live or :paused for a column; a whole transform reports its
  # lifecycle status (see Status). paused_at (a Time) and last_error are set
  # only for a paused column: when its pause tripped, and its most recent
  # failure (nil for a pause that cascaded from an upstream column rather
  # than failing on its own).
  QuarantineEntry = Data.define(:target, :state, :paused_at, :last_error) do
    def self.from_native(hash)
      micros = hash[:paused_at_micros]
      new(**hash.except(:paused_at_micros), paused_at: micros && EpochMicros.to_time(micros))
    end
  end

  # One source row the apply path gave up on, as Trellis.poisoned_since
  # lists it: its fully-qualified src_table, its key as Trellis renders it,
  # the error that poisoned it, and when (poisoned_at, a Time).
  PoisonEntry = Data.define(:src_table, :key, :last_error, :poisoned_at) do
    def self.from_native(hash)
      new(**hash.except(:poisoned_at_micros),
          poisoned_at: EpochMicros.to_time(hash.fetch(:poisoned_at_micros)))
    end
  end

  # One quarantined source row, as Trellis.sample_quarantined pages them: its
  # fully-qualified src_table, its key as Trellis renders it, and the error
  # it failed with.
  PoisonSample = Data.define(:src_table, :key, :error_message)

  # One page of Trellis.sample_quarantined, ordered by (src_table, key).
  #
  # next_cursor is opaque: pass it back as the `after:` option for the next
  # page, and don't read or build one. The last page is the one with fewer
  # rows than the limit. An empty page hands back the cursor it was asked
  # for, so polling from where you got to never falls back to the first page;
  # next_cursor is nil only for an empty first page.
  SamplePage = Data.define(:samples, :next_cursor) do
    def self.from_native(hash)
      new(samples: hash.fetch(:samples).map { |sample| PoisonSample.new(**sample) },
          next_cursor: hash.fetch(:next_cursor))
    end
  end

  # What Trellis.self_check found auditing one page of a target table
  # against a fresh recompute of its definition from the source.
  #
  # - outcome: :converged (every compared cell, row and column matched),
  #   :not_caught_up (the target didn't catch up within timeout_ms: not a
  #   verdict on correctness, and nothing was compared), or :diverged (see
  #   divergences).
  # - divergences: the Divergences found; [] unless outcome is :diverged.
  # - rows_compared: the distinct keys compared; 0 when the target didn't
  #   catch up.
  # - next_after: the opaque cursor to pass as the next call's `after:` to
  #   audit the following page. nil means this page reached the end of the
  #   target. A page that fills `limit:` exactly still returns a cursor, so a
  #   sweep can end on a page that compares nothing.
  # - checked_through: the watermark the outcome holds through, a token
  #   Trellis.await_converged takes.
  SelfCheckReport = Data.define(:target, :outcome, :divergences, :rows_compared, :next_after,
                                :checked_through) do
    def self.from_native(hash)
      new(**hash, divergences: hash.fetch(:divergences).map { |d| Divergence.new(**d) })
    end
  end

  # One thing Trellis.self_check found out of step, by kind:
  #
  # - :cell: the row exists on both sides but `column` differs at `key`.
  #   `persisted` is the target table's value and `recomputed` the fresh
  #   recompute's, both as Postgres renders them as text (nil for NULL).
  # - :missing_row: the recompute produced `key`, but the target has no row
  #   for it.
  # - :extra_row: the target has a row for `key` that the recompute didn't
  #   produce.
  # - :missing_column: the definition expects `column`, but the target table
  #   doesn't have it.
  # - :extra_column: the target table has `column`, but the definition
  #   doesn't expect it.
  # - :capture: a table the target is computed from isn't being captured as
  #   Trellis installed it (a capture trigger missing, disabled or calling
  #   the wrong function, a capture function missing or with the wrong
  #   owner, the Trellis role missing a privilege, or the table joined a
  #   partition or inheritance hierarchy). `detail` says what, and `table`
  #   names the table (nil for a missing privilege). self_check reports these
  #   before, and instead of, comparing any rows, so rows_compared is 0.
  #
  # Fields a kind doesn't carry are nil.
  Divergence = Data.define(:kind, :key, :column, :persisted, :recomputed, :table, :detail)

  # The configuration this process's handle connected with, as Trellis.config
  # returns it: connect's schema and target_schema, and the connection pool's
  # size cap and how long a call waits for a free connection.
  #
  # Not the url: a connection string can carry a password, and a Config is
  # the kind of value that ends up in a log line whole. The caller already
  # has the url it connected with.
  Config = Data.define(:schema, :target_schema, :pool_max_size, :pool_wait_timeout_ms)
end
