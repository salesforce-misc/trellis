# frozen_string_literal: true

require "test_helper"

# Trellis.self_check end to end: start a background check of a live target
# that matches its source, read the job back by id, then corrupt one target
# cell behind the engine's back and see the check name it. Mirrors the Elixir
# suite's self_check_test.exs.
class SelfCheckTest < Minitest::Test
  include TrellisTestCase

  TIMEOUT_MS = 30_000

  def test_a_matching_target_converges_and_a_corrupted_cell_diverges
    pg = TestCluster.pg
    pg.exec("create table audited_widgets (id integer primary key, price integer, tax integer)")
    Trellis.connect(url: TestCluster.dsn, staging: true, drain_threads: 1)

    Trellis.apply("TRANSFORM audited_widget_totals FROM audited_widgets SELECT price + tax AS total")
    await_status("audited_widget_totals")

    pg.exec("insert into audited_widgets (id, price, tax) values (1, 10, 1), (2, 20, 2)")
    Trellis.await_converged(Trellis.watermark_token, timeout_ms: TIMEOUT_MS)

    # The call returns the job; a drain worker runs it.
    started = Trellis.self_check("audited_widget_totals", timeout_ms: TIMEOUT_MS)
    assert_instance_of Trellis::SelfCheckJob, started
    assert_equal ["audited_widget_totals", :standard], [started.target, started.mode]
    assert_includes %i[queued running done], started.state

    job = await_job(started.id)
    assert_equal [:done, 2, nil], [job.state, job.rows_compared, job.error]
    assert_predicate job, :finished?
    report = job.report
    assert_instance_of Trellis::SelfCheckReport, report
    assert_equal ["audited_widget_totals", :converged, [], 2, false, []],
                 [report.target, report.outcome, report.divergences, report.rows_compared,
                  report.truncated, report.drain_failures]

    # The position it checked through is a real watermark token.
    assert_nil Trellis.await_converged(report.checked_through, timeout_ms: TIMEOUT_MS)

    # The engine wrote 11 (10 + 1); this test overwrites it.
    pg.exec("update audited_widget_totals set total = 9999 where id = '1'")

    # A new check replaces the finished one: its id is gone.
    job = await_job(Trellis.self_check("audited_widget_totals", timeout_ms: TIMEOUT_MS,
                                                                  mode: :strict).id)
    assert_nil Trellis.self_check_job(started.id)
    assert_equal :diverged, job.report.outcome
    assert_equal [Trellis::Divergence.new(kind: :cell, key: "1", column: "total",
                                          persisted: "9999", recomputed: "11",
                                          table: nil, detail: nil)],
                 job.report.divergences

    # A source attached as a partition is no longer captured whole, which the
    # capture audit reports before it compares anything. The staging worker
    # doesn't undo an ATTACH, so the report is stable.
    pg.exec("create table audited_widgets_all (id integer not null, price integer, tax integer) " \
            "partition by range (id)")
    pg.exec("alter table audited_widgets_all attach partition audited_widgets " \
            "for values from (minvalue) to (maxvalue)")
    report = await_job(Trellis.self_check("audited_widget_totals", timeout_ms: TIMEOUT_MS).id).report
    assert_equal [:diverged, 0], [report.outcome, report.rows_compared]
    assert_equal [[:capture, "public.audited_widgets"]],
                 report.divergences.map { |d| [d.kind, d.table] }
    assert_match "partition of public.audited_widgets_all", report.divergences.first.detail
    pg.exec("alter table audited_widgets_all detach partition audited_widgets")
  ensure
    pg&.close
  end

  # With no drain worker anywhere, a job is registered and stays queued, and
  # a second call for the target gets the same job back.
  def test_a_job_no_worker_runs_stays_queued_and_a_second_call_returns_it
    pg = TestCluster.pg
    pg.exec("create table queued_widgets (id integer primary key, price integer)")
    Trellis.connect(url: TestCluster.dsn)
    Trellis.apply("TRANSFORM queued_widget_prices FROM queued_widgets SELECT price AS price")

    job = Trellis.self_check("queued_widget_prices", timeout_ms: 1_000, mode: :strict)
    assert_equal [:queued, :strict, 0, nil, nil],
                 [job.state, job.mode, job.rows_compared, job.report, job.error]
    refute_predicate job, :finished?

    again = Trellis.self_check("queued_widget_prices", timeout_ms: 5_000)
    assert_equal job, again, "the unfinished job comes back, whatever mode this call asked for"
    assert_equal job, Trellis.self_check_job(job.id)
    assert_nil Trellis.self_check_job(job.id + 1_000)
  ensure
    pg&.close
  end

  def test_an_unknown_target_is_not_found_and_bad_options_are_validation_errors
    Trellis.connect(url: TestCluster.dsn)

    error = assert_raises(Trellis::NotFoundError) do
      Trellis.self_check("no_such_target", timeout_ms: 1_000)
    end
    assert_match "no_such_target", error.message

    [
      { timeout_ms: -1 },
      { timeout_ms: 2**64 },
      { timeout_ms: 1_000, mode: :lenient },
      { timeout_ms: 1_000, mode: "strict" }
    ].each do |options|
      assert_raises(Trellis::ValidationError, options.inspect) do
        Trellis.self_check("no_such_target", **options)
      end
    end
    [{}, { limit: 10, timeout_ms: 1_000 }, { timeout_ms: 1_000, after: "1" }].each do |options|
      assert_raises(ArgumentError, options.inspect) do
        Trellis.self_check("no_such_target", **options)
      end
    end
    ["1", nil, 2**63].each do |id|
      assert_raises(Trellis::ValidationError, id.inspect) { Trellis.self_check_job(id) }
    end
  end

  private

  # The job `id`, once it has finished.
  def await_job(id)
    eventually_value("self_check job #{id} to finish", ->(job) { job&.finished? }) do
      Trellis.self_check_job(id)
    end
  end
end
