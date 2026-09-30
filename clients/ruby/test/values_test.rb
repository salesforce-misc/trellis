# frozen_string_literal: true

require "test_helper"

# The Ruby half of the plain-data conversions: the extension's hashes into
# `Data` values, and the closed symbol sets the values document. No database.
class ValuesTest < Minitest::Test
  # 2024-09-25 00:00:00.654321 UTC.
  MICROS = 1_727_222_400_654_321

  def test_times_cross_as_epoch_microseconds_and_become_utc_times
    time = Trellis::EpochMicros.to_time(MICROS)
    assert time.utc?
    assert_equal Time.utc(2024, 9, 25, 0, 0, Rational(654_321, 1_000_000)), time
    assert_equal MICROS, Trellis::EpochMicros.from_time(time)

    # A time in another zone is the same instant.
    assert_equal MICROS, Trellis::EpochMicros.from_time(time.getlocal("+05:30"))
    # Sub-microsecond precision truncates toward the past, before the epoch too.
    assert_equal MICROS, Trellis::EpochMicros.from_time(time + Rational(1, 10**9))
    assert_equal(-1, Trellis::EpochMicros.from_time(Time.at(0) - Rational(1, 10**9)))
    assert_nil Trellis::EpochMicros.from_time(Time.at(2**62))
  end

  def test_a_backfill_failures_retry_time_becomes_a_time
    status = Trellis::Status.from_native(
      status: :waiting_to_backfill,
      backfill_failure: { source_table: "public.orders", attempts: 3,
                          last_error: "permission denied", next_attempt_at_micros: MICROS }
    )
    assert_equal Trellis::Status.new(
      status: :waiting_to_backfill,
      backfill_failure: Trellis::BackfillFailure.new(
        source_table: "public.orders", attempts: 3, last_error: "permission denied",
        next_attempt_at: Trellis::EpochMicros.to_time(MICROS)
      )
    ), status
  end

  def test_a_definition_summarys_backfill_failure_becomes_a_backfill_failure
    summary = Trellis::DefinitionSummary.from_native(
      id: 1, target_table: "public.t", source_table: "public.s", source_version: 1,
      status: :waiting_to_backfill, created_at_micros: MICROS,
      backfill_failure: { source_table: "public.s", attempts: 2, last_error: "timeout",
                          next_attempt_at_micros: MICROS }
    )
    assert_equal Trellis::DefinitionSummary.new(
      id: 1, target_table: "public.t", source_table: "public.s", source_version: 1,
      status: :waiting_to_backfill, created_at: Trellis::EpochMicros.to_time(MICROS),
      backfill_failure: Trellis::BackfillFailure.new(
        source_table: "public.s", attempts: 2, last_error: "timeout",
        next_attempt_at: Trellis::EpochMicros.to_time(MICROS)
      )
    ), summary
  end

  def test_an_applied_carries_only_its_kinds_fields
    definition = { id: 1, target_table: "public.t", source_table: "public.s",
                   source_version: 1, status: :waiting_to_backfill, source_columns: {} }
    empty = { definition: nil, relationship: nil, columns: nil, added: nil, dropped: nil,
              altered: nil }

    applied = Trellis::Applied.from_native(empty.merge(kind: :transform_defined, definition:))
    assert_equal Trellis::Definition.new(**definition), applied.definition

    altered = Trellis::Applied.from_native(
      empty.merge(kind: :altered, definition:, added: ["a"], dropped: [], altered: ["b"])
    )
    assert_equal [["a"], [], ["b"]], [altered.added, altered.dropped, altered.altered]
    assert_instance_of Trellis::Definition, altered.definition

    # An outcome newer than the binding is still a value, not a crash.
    unknown = Trellis::Applied.from_native(empty.merge(kind: :unknown))
    assert_equal :unknown, unknown.kind

    # And it pattern-matches by kind.
    resumed = Trellis::Applied.from_native(empty.merge(kind: :resumed, columns: ["t.c"]))
    case resumed
    in { kind: :resumed, columns: } then assert_equal ["t.c"], columns
    end
  end

  def test_a_quarantine_entry_has_a_pause_time_only_when_paused
    live = Trellis::QuarantineEntry.from_native(target: "t.c", state: :live,
                                                paused_at_micros: nil, last_error: nil)
    assert_nil live.paused_at

    paused = Trellis::QuarantineEntry.from_native(target: "t.c", state: :paused,
                                                  paused_at_micros: MICROS, last_error: "boom")
    assert_equal Trellis::EpochMicros.to_time(MICROS), paused.paused_at
  end

  # The symbols the value classes document are exactly the ones the
  # extension can return, which it interns from the engine's own lists.
  def test_the_documented_symbols_are_exactly_the_engines
    assert_equal %i[waiting_to_backfill backfilling catching_up live quarantined paused],
                 Trellis::Native.status_names
    assert_equal %i[live waiting_to_backfill backfilling catching_up quarantined paused].sort,
                 Trellis::Native.quarantine_states.sort
    assert_equal %i[one many], Trellis::Native.cardinality_names
    assert_equal %i[transform_defined relationship_defined paused resumed dropped altered
                    unknown],
                 Trellis::Native.applied_kinds
    assert_equal %i[converged not_caught_up diverged], Trellis::Native.self_check_outcomes
    assert_equal %i[cell missing_row extra_row missing_column extra_column capture],
                 Trellis::Native.divergence_kinds
  end
end
