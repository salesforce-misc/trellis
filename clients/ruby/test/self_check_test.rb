# frozen_string_literal: true

require "test_helper"

# Trellis.self_check end to end: audit a live target that matches its
# source, page through it, then corrupt one target cell behind the engine's
# back and see the audit name it. Mirrors the Elixir suite's
# self_check_test.exs.
class SelfCheckTest < Minitest::Test
  include TrellisTestCase

  TIMEOUT_MS = 30_000

  def test_a_matching_target_converges_pages_by_next_after_and_a_corrupted_cell_diverges
    pg = TestCluster.pg
    pg.exec("create table audited_widgets (id integer primary key, price integer, tax integer)")
    Trellis.connect(url: TestCluster.dsn, staging: true, drain_threads: 1)

    Trellis.apply("TRANSFORM audited_widget_totals FROM audited_widgets SELECT price + tax AS total")
    await_status("audited_widget_totals")

    pg.exec("insert into audited_widgets (id, price, tax) values (1, 10, 1), (2, 20, 2)")
    Trellis.await_converged(Trellis.watermark_token, timeout_ms: TIMEOUT_MS)

    report = Trellis.self_check("audited_widget_totals", limit: 100, timeout_ms: TIMEOUT_MS)
    assert_instance_of Trellis::SelfCheckReport, report
    assert_equal ["audited_widget_totals", :converged, [], 2, nil, [], []],
                 [report.target, report.outcome, report.divergences, report.rows_compared,
                  report.next_after, report.drain_failures, report.unindexed_joins]

    # The position it checked through is a real watermark token.
    assert_nil Trellis.await_converged(report.checked_through, timeout_ms: TIMEOUT_MS)

    # One key a page: sweeping through next_after visits each key once and
    # ends on a nil cursor. A page that fills its limit exactly still hands
    # back a cursor (the engine can't know no key follows), so the sweep ends
    # on an empty third page.
    pages = sweep("audited_widget_totals", 1)
    assert_equal [[1, true], [1, true], [0, false]],
                 pages.map { |page| [page.rows_compared, page.next_after.is_a?(String)] }
    assert(pages.all? { |page| page.outcome == :converged })

    # The engine wrote 11 (10 + 1); this test overwrites it.
    pg.exec("update audited_widget_totals set total = 9999 where id = '1'")

    report = Trellis.self_check("audited_widget_totals", limit: 100, timeout_ms: TIMEOUT_MS)
    assert_equal :diverged, report.outcome
    assert_equal [Trellis::Divergence.new(kind: :cell, key: "1", column: "total",
                                          persisted: "9999", recomputed: "11",
                                          table: nil, detail: nil)],
                 report.divergences

    # A source attached as a partition is no longer captured whole, which the
    # capture audit reports before it compares anything. The staging worker
    # doesn't undo an ATTACH, so the report is stable.
    pg.exec("create table audited_widgets_all (id integer not null, price integer, tax integer) " \
            "partition by range (id)")
    pg.exec("alter table audited_widgets_all attach partition audited_widgets " \
            "for values from (minvalue) to (maxvalue)")
    report = Trellis.self_check("audited_widget_totals", limit: 100, timeout_ms: TIMEOUT_MS)
    assert_equal [:diverged, 0], [report.outcome, report.rows_compared]
    assert_equal [[:capture, "public.audited_widgets"]],
                 report.divergences.map { |d| [d.kind, d.table] }
    assert_match "partition of public.audited_widgets_all", report.divergences.first.detail
    pg.exec("alter table audited_widgets_all detach partition audited_widgets")
  ensure
    pg&.close
  end

  def test_an_unknown_target_is_not_found_and_bad_options_are_validation_errors
    Trellis.connect(url: TestCluster.dsn)

    error = assert_raises(Trellis::NotFoundError) do
      Trellis.self_check("no_such_target", limit: 10, timeout_ms: 1_000)
    end
    assert_match "no_such_target", error.message

    [
      { limit: 0, timeout_ms: 1_000 },
      { limit: 2**63, timeout_ms: 1_000 },
      { limit: 10, timeout_ms: -1 },
      { limit: 10, timeout_ms: 1_000, mode: :lenient },
      { limit: 10, timeout_ms: 1_000, mode: "strict" },
      { limit: 10, timeout_ms: 1_000, after: 5 }
    ].each do |options|
      assert_raises(Trellis::ValidationError, options.inspect) do
        Trellis.self_check("no_such_target", **options)
      end
    end
    [{ timeout_ms: 1_000 }, { limit: 10 }, { limit: 10, timeout_ms: 1_000, page: 2 }]
      .each do |options|
        assert_raises(ArgumentError, options.inspect) do
          Trellis.self_check("no_such_target", **options)
        end
      end
  end

  private

  # Every page of a strict audit of target, `limit` keys at a time, through
  # to the one whose next_after is nil. Bounded, so a cursor that never
  # reaches the end fails rather than looping.
  def sweep(target, limit)
    pages = []
    cursor = nil
    20.times do
      report = Trellis.self_check(target, limit:, timeout_ms: TIMEOUT_MS, mode: :strict,
                                          after: cursor)
      pages << report
      return pages if report.next_after.nil?

      cursor = report.next_after
    end
    flunk "the sweep of #{target} never ended; last saw #{pages.last.inspect}"
  end
end
