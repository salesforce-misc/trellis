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
                          last_error: "permission denied", next_attempt_at_micros: MICROS },
      capture_wait: nil, capture_failure: nil, held_keys: nil, drain_failure: nil,
      source_index_gaps: [], correcting: [], paused_upstream_columns: [],
      build_wait_xid: nil, version_handover: nil, join: nil,
      floor: "0.2.0"
    )
    assert_equal Trellis::Status.new(
      status: :waiting_to_backfill,
      backfill_failure: Trellis::BackfillFailure.new(
        source_table: "public.orders", attempts: 3, last_error: "permission denied",
        next_attempt_at: Trellis::EpochMicros.to_time(MICROS)
      ),
      capture_wait: nil,
      capture_failure: nil,
      held_keys: nil,
      drain_failure: nil,
      source_index_gaps: [],
      correcting: [], paused_upstream_columns: [], build_wait_xid: nil, version_handover: nil,
      below_catalog_floor: nil, join: nil,
      floor: "0.2.0"
    ), status
  end

  def test_a_capture_wait_a_capture_failure_and_held_keys_become_values_with_times
    status = Trellis::Status.from_native(
      status: :catching_up, backfill_failure: nil,
      capture_wait: { table: "public.orders", operation: "widen",
                      lock_mode: "ShareRowExclusiveLock", waiting_since_micros: MICROS,
                      observed_at_micros: MICROS + 1, blockers: ["pid 42 holds RowExclusiveLock"] },
      capture_failure: { kind: :capture, source_table: "public.lines", columns: ["qty"],
                         error: "column \"qty\" was dropped", detected_at_micros: MICROS },
      held_keys: { count: 2, oldest_poisoned_at_micros: MICROS + 2 },
      drain_failure: nil, source_index_gaps: [], correcting: [], paused_upstream_columns: [],
      build_wait_xid: nil, version_handover: nil, join: nil,
      floor: "0.2.0"
    )
    assert_equal Trellis::Status.new(
      status: :catching_up, backfill_failure: nil,
      capture_wait: Trellis::CaptureWait.new(
        table: "public.orders", operation: "widen", lock_mode: "ShareRowExclusiveLock",
        waiting_since: Trellis::EpochMicros.to_time(MICROS),
        observed_at: Trellis::EpochMicros.to_time(MICROS + 1),
        blockers: ["pid 42 holds RowExclusiveLock"]
      ),
      capture_failure: Trellis::CaptureFailure.new(
        kind: :capture, source_table: "public.lines", columns: ["qty"], error: "column \"qty\" was dropped",
        detected_at: Trellis::EpochMicros.to_time(MICROS)
      ),
      held_keys: Trellis::HeldKeys.new(count: 2, oldest_poisoned_at: Trellis::EpochMicros.to_time(MICROS + 2)),
      drain_failure: nil,
      source_index_gaps: [],
      correcting: [], paused_upstream_columns: [], build_wait_xid: nil, version_handover: nil,
      below_catalog_floor: nil, join: nil,
      floor: "0.2.0"
    ), status
  end

  def test_a_drain_failure_becomes_a_value_with_times
    status = Trellis::Status.from_native(
      status: :live, backfill_failure: nil, capture_wait: nil, capture_failure: nil, held_keys: nil,
      drain_failure: { seg_seq: 17, tables: ["public.lines", "public.orders"],
                       error: "permission denied for function audit_hook", sqlstate: "42501",
                       since_micros: MICROS, last_seen_micros: MICROS + 1, attempts: 5 },
      source_index_gaps: [], correcting: [], paused_upstream_columns: [],
      build_wait_xid: nil, version_handover: nil, join: nil,
      floor: "0.2.0"
    )
    assert_equal Trellis::DrainFailure.new(
      seg_seq: 17, tables: ["public.lines", "public.orders"],
      error: "permission denied for function audit_hook", sqlstate: "42501",
      since: Trellis::EpochMicros.to_time(MICROS), last_seen: Trellis::EpochMicros.to_time(MICROS + 1),
      attempts: 5
    ), status.drain_failure
  end

  def test_a_not_live_self_check_report_names_the_status_it_found
    report = Trellis::SelfCheckReport.from_native(
      target: "order_totals", outcome: :not_live, status: :backfilling, divergences: [],
      rows_compared: 0, truncated: false, checked_through: "1/16B3748", held_keys: nil,
      drain_failures: []
    )
    assert_equal :not_live, report.outcome
    assert_equal :backfilling, report.status
  end

  def test_a_self_check_reports_drain_failures_become_values_whatever_the_outcome
    report = Trellis::SelfCheckReport.from_native(
      target: "order_totals", outcome: :converged, status: nil, divergences: [], rows_compared: 2,
      truncated: false, checked_through: "1/16B3748", held_keys: nil,
      drain_failures: [{ seg_seq: 9, tables: ["public.orders"], error: "records fail only together",
                         sqlstate: nil, since_micros: MICROS, last_seen_micros: MICROS, attempts: 1 }]
    )
    assert_equal [Trellis::DrainFailure.new(
      seg_seq: 9, tables: ["public.orders"], error: "records fail only together", sqlstate: nil,
      since: Trellis::EpochMicros.to_time(MICROS), last_seen: Trellis::EpochMicros.to_time(MICROS),
      attempts: 1
    )], report.drain_failures
  end

  def test_a_source_index_gap_becomes_a_value_in_a_status
    gap = { table: "public.orders", columns: ["customer_id"], role: :join_joined, opted_out: false,
            fix: 'create index on "public"."orders" ("customer_id")' }
    status = Trellis::Status.from_native(
      status: :live, backfill_failure: nil, capture_wait: nil, capture_failure: nil, held_keys: nil,
      drain_failure: nil, source_index_gaps: [gap], correcting: [], paused_upstream_columns: [],
      build_wait_xid: nil, version_handover: nil, join: nil,
      floor: "0.2.0"
    )
    assert_equal [Trellis::SourceIndexGap.new(**gap)], status.source_index_gaps
  end

  def test_a_node_under_correction_becomes_a_value_with_its_queue_time
    status = Trellis::Status.from_native(
      status: :backfilling, backfill_failure: nil, capture_wait: nil, capture_failure: nil,
      held_keys: nil, drain_failure: nil, source_index_gaps: [],
      correcting: [{ node: "public.items_p1", since_micros: MICROS, keys_remaining: 5 },
                   { node: "public.items_p2", since_micros: MICROS + 1, keys_remaining: nil }],
      paused_upstream_columns: [], build_wait_xid: nil, version_handover: nil, join: nil,
      floor: "0.2.0"
    )
    assert_equal :backfilling, status.status
    assert_equal [
      Trellis::CorrectingNode.new(node: "public.items_p1", since: Trellis::EpochMicros.to_time(MICROS),
                                  keys_remaining: 5),
      Trellis::CorrectingNode.new(node: "public.items_p2", since: Trellis::EpochMicros.to_time(MICROS + 1),
                                  keys_remaining: nil)
    ], status.correcting
  end

  def test_the_paused_upstream_columns_of_a_reader_are_kept_as_strings
    status = Trellis::Status.from_native(
      status: :catching_up, backfill_failure: nil, capture_wait: nil, capture_failure: nil,
      held_keys: nil, drain_failure: nil, source_index_gaps: [],
      correcting: [], paused_upstream_columns: ["t.total"],
      build_wait_xid: nil, version_handover: nil, join: nil,
      floor: "0.2.0"
    )
    assert_equal :catching_up, status.status
    assert_equal ["t.total"], status.paused_upstream_columns
  end

  def test_a_build_waiting_on_a_fence_keeps_its_transaction_id
    status = Trellis::Status.from_native(
      status: :backfilling, backfill_failure: nil, capture_wait: nil, capture_failure: nil,
      held_keys: nil, drain_failure: nil, source_index_gaps: [],
      correcting: [], paused_upstream_columns: [],
      build_wait_xid: 4_294_967_301, version_handover: nil, join: nil,
      floor: "0.2.0"
    )
    assert_equal 4_294_967_301, status.build_wait_xid
    assert_nil status.version_handover
  end

  def test_a_version_handover_becomes_a_value_naming_the_other_processes
    status = Trellis::Status.from_native(
      status: :live, backfill_failure: nil, capture_wait: nil, capture_failure: nil,
      held_keys: nil, drain_failure: nil, source_index_gaps: [],
      correcting: [], paused_upstream_columns: [], build_wait_xid: nil,
      version_handover: {
        this_version: "0.2.0",
        processes: [{ worker_id: "trellis-host-17-ab12", version: "0.1.0" },
                    { worker_id: "trellis-host-18-cd34", version: nil }],
        unregistered_staging_worker: true, migrations_pending: false
      },
      join: nil,
      floor: "0.2.0"
    )
    assert_nil status.build_wait_xid
    assert_equal Trellis::VersionHandover.new(
      this_version: "0.2.0",
      processes: [
        Trellis::OtherProcess.new(worker_id: "trellis-host-17-ab12", version: "0.1.0"),
        Trellis::OtherProcess.new(worker_id: "trellis-host-18-cd34", version: nil)
      ],
      unregistered_staging_worker: true, migrations_pending: false
    ), status.version_handover
  end

  def test_a_catalog_floor_becomes_a_value_naming_the_floor_and_this_version
    status = Trellis::Status.from_native(
      status: :live, backfill_failure: nil, capture_wait: nil, capture_failure: nil,
      held_keys: nil, drain_failure: nil, source_index_gaps: [],
      correcting: [], paused_upstream_columns: [], build_wait_xid: nil,
      version_handover: nil, below_catalog_floor: { floor: "0.3.0", this_version: "0.2.0" },
      join: nil, floor: "0.2.0"
    )
    assert_equal Trellis::BelowCatalogFloor.new(floor: "0.3.0", this_version: "0.2.0"),
                 status.below_catalog_floor
  end

  def test_a_join_status_keeps_its_tables_warnings_and_build_step
    status = Trellis::Status.from_native(
      status: :live, backfill_failure: nil, capture_wait: nil, capture_failure: nil,
      held_keys: nil, drain_failure: nil, source_index_gaps: [],
      correcting: [], paused_upstream_columns: [], build_wait_xid: nil, version_handover: nil,
      join: {
        hidden_tables: [
          { table: "P", name: "join_7_p", estimated_rows: 5_000_000_011, bytes: 9_000_000_017 },
          { table: "M", name: "join_7_m", estimated_rows: 21, bytes: 8192 }
        ],
        partials_warning: { partials_rows: 6_000_000_013, fact_rows: 7_000_000_019 },
        fan_out_warning: { key: "42,eu", rows: 3_000_000_023, threshold: 250_000 },
        building: { step: :side, side: :joined, part: :leaf, leaf: "public.items_p2",
                    remaining: nil }
      },
      floor: "0.2.0"
    )
    assert_equal Trellis::JoinStatus.new(
      hidden_tables: [
        Trellis::HiddenTable.new(table: "P", name: "join_7_p",
                                 estimated_rows: 5_000_000_011, bytes: 9_000_000_017),
        Trellis::HiddenTable.new(table: "M", name: "join_7_m", estimated_rows: 21, bytes: 8192)
      ],
      partials_warning: Trellis::PartialsWarning.new(partials_rows: 6_000_000_013,
                                                     fact_rows: 7_000_000_019),
      fan_out_warning: Trellis::FanOutWarning.new(key: "42,eu", rows: 3_000_000_023,
                                                  threshold: 250_000),
      building: Trellis::JoinBuildStep.new(step: :side, side: :joined, part: :leaf,
                                           leaf: "public.items_p2", remaining: nil)
    ), status.join
  end

  def test_a_one_to_one_joins_chunk_step_keeps_how_many_chunks_remain
    status = Trellis::Status.from_native(
      status: :backfilling, backfill_failure: nil, capture_wait: nil, capture_failure: nil,
      held_keys: nil, drain_failure: nil, source_index_gaps: [],
      correcting: [], paused_upstream_columns: [], build_wait_xid: nil, version_handover: nil,
      join: {
        hidden_tables: [], partials_warning: nil, fan_out_warning: nil,
        building: { step: :chunks, side: nil, part: nil, leaf: nil, remaining: 4_294_967_301 }
      },
      floor: "0.2.0"
    )
    assert_equal Trellis::JoinStatus.new(
      hidden_tables: [], partials_warning: nil, fan_out_warning: nil,
      building: Trellis::JoinBuildStep.new(step: :chunks, side: nil, part: nil, leaf: nil,
                                           remaining: 4_294_967_301)
    ), status.join
  end

  def test_a_definitions_floor_keeps_the_release_it_names
    status = Trellis::Status.from_native(
      status: :live, backfill_failure: nil, capture_wait: nil, capture_failure: nil,
      held_keys: nil, drain_failure: nil, source_index_gaps: [],
      correcting: [], paused_upstream_columns: ["upstream.amount"], build_wait_xid: nil,
      version_handover: nil, below_catalog_floor: nil, join: nil, floor: "0.3.1"
    )
    assert_equal "0.3.1", status.floor
    assert_equal ["upstream.amount"], status.paused_upstream_columns
  end

  def test_a_definition_summarys_backfill_failure_becomes_a_backfill_failure
    summary = Trellis::DefinitionSummary.from_native(
      target_table: "public.t", status: :waiting_to_backfill, created_at_micros: MICROS,
      backfill_failure: { source_table: "public.s", attempts: 2, last_error: "timeout",
                          next_attempt_at_micros: MICROS },
      halt: nil
    )
    assert_equal Trellis::DefinitionSummary.new(
      target_table: "public.t", status: :waiting_to_backfill,
      created_at: Trellis::EpochMicros.to_time(MICROS),
      backfill_failure: Trellis::BackfillFailure.new(
        source_table: "public.s", attempts: 2, last_error: "timeout",
        next_attempt_at: Trellis::EpochMicros.to_time(MICROS)
      ),
      halt: nil
    ), summary
  end

  def test_a_definition_summarys_halt_becomes_a_capture_failure_of_kind_halt
    summary = Trellis::DefinitionSummary.from_native(
      target_table: "public.t", status: :paused, created_at_micros: MICROS, backfill_failure: nil,
      halt: { kind: :halt, source_table: "public.s", columns: [],
              error: "the drain halted", detected_at_micros: MICROS }
    )
    assert_equal Trellis::CaptureFailure.new(
      kind: :halt, source_table: "public.s", columns: [], error: "the drain halted",
      detected_at: Trellis::EpochMicros.to_time(MICROS)
    ), summary.halt
  end

  def test_an_applied_carries_only_its_kinds_fields
    empty = { target_table: nil, columns: nil, added: nil, dropped: nil, altered: nil }

    applied = Trellis::Applied.from_native(empty.merge(kind: :transform_defined, target_table: "public.t"))
    assert_equal "public.t", applied.target_table

    altered = Trellis::Applied.from_native(
      empty.merge(kind: :altered, target_table: "public.t", added: ["a"], dropped: [], altered: ["b"])
    )
    assert_equal [["a"], [], ["b"]], [altered.added, altered.dropped, altered.altered]
    assert_equal "public.t", altered.target_table

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
    assert_equal %i[transform_defined paused resumed dropped altered unknown],
                 Trellis::Native.applied_kinds
    assert_equal %i[converged not_caught_up not_live diverged], Trellis::Native.self_check_outcomes
    assert_equal %i[cell missing_row extra_row missing_column extra_column capture],
                 Trellis::Native.divergence_kinds
    assert_equal %i[capture halt disruption], Trellis::Native.capture_failure_kinds
    assert_equal %i[group_by join_from join_joined], Trellis::Native.index_roles
    assert_equal %i[whole side final joined_rows joined_null_keys joined_gone planning_chunks
                    chunks sweep_stale sweep_orphans unknown],
                 Trellis::Native.join_build_steps
    assert_equal %i[from joined unknown], Trellis::Native.join_sides
    assert_equal %i[whole leaf remainder unroutable unknown], Trellis::Native.join_side_parts
  end
end
