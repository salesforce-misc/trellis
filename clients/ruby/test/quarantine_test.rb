# frozen_string_literal: true

require "test_helper"

# The quarantine path has the most structure crossing the native boundary
# (addresses, state symbols, times, the opaque cursor), so this drives it end
# to end through real source rows: poison a column, read it back every way
# the API offers, resume it with a statement, and see it clear. Mirrors the
# Elixir suite's quarantine_test.exs.
class QuarantineTest < Minitest::Test
  include TrellisTestCase

  # The column fuse trips once this many distinct rows have failed on one
  # column (`staging::quarantine::DEFAULT_COLUMN_DEATH_THRESHOLD`).
  COLUMN_DEATH_THRESHOLD = 5

  def test_a_poisoned_column_is_listed_sampled_page_by_page_resumed_and_cleared
    pg = TestCluster.pg
    pg.exec("create table ledger (id integer primary key, n integer)")
    Trellis.connect(url: TestCluster.dsn, staging: true, drain_threads: 1)

    # `integer + integer` is an `integer`, so doubling anything past 2^30
    # overflows it, on the engine exactly as on Postgres.
    applied = Trellis.apply("TRANSFORM doubled_ledger FROM ledger SELECT n + n AS doubled")
    assert_equal :transform_defined, applied.kind
    await_status("doubled_ledger")
    assert_equal [], Trellis.quarantined

    # One statement, so all five rows land in one batch and its isolation
    # pass charges the column once per row. Split across batches, the first
    # row's batch fails alone on each drain cycle, and the row-level fuse
    # (also five) evicts the key before the column fuse can trip: a
    # poisoned key no public call releases, which wedges convergence for
    # every test after this one (#588).
    ids = (1..COLUMN_DEATH_THRESHOLD).to_a
    pg.exec_params("insert into ledger (id, n) select id, 2000000000 from generate_series(1, $1::int) id",
                   [COLUMN_DEATH_THRESHOLD])

    paused = eventually_value("doubled_ledger.doubled to pause",
                              ->(entry) { entry.state == :paused }) do
      Trellis.quarantine_status("doubled_ledger.doubled")
    end
    assert_instance_of Trellis::QuarantineEntry, paused
    assert_equal "doubled_ledger.doubled", paused.target
    assert_match "out of range", paused.last_error
    assert_instance_of Time, paused.paused_at
    assert paused.paused_at.utc?
    assert_in_delta Time.now, paused.paused_at, 60

    # The pause is visible while the batch that tripped it can still be in
    # flight, and a resume that lands before that batch settles sees it
    # retried against its original row images, which pauses the column again
    # (#585). Wait for everything written so far to drain first, the way an
    # operator resuming a column has to until that's fixed.
    Trellis.await_converged(Trellis.watermark_token, timeout_ms: 30_000)

    # The flat list carries the same entry, by the same address.
    assert_includes Trellis.quarantined, paused

    # The transform itself stays live: a column pause is the column's alone.
    transform = Trellis.quarantine_status("doubled_ledger")
    assert_equal ["doubled_ledger", :live], [transform.target, transform.state]

    # Two rows a page, so the cursor has to carry three pages.
    pages = sample_pages("doubled_ledger.doubled", 2)
    assert_equal [2, 2, 1, 0], pages.map { |page| page.samples.length }

    samples = pages.flat_map(&:samples)
    assert_equal ids.map(&:to_s), samples.map(&:key)
    samples.each do |sample|
      assert_instance_of Trellis::PoisonSample, sample
      assert_equal "public.ledger", sample.src_table
      assert_match "out of range", sample.error_message
    end

    # Polling past the end hands back the cursor it was asked for, not nil,
    # so the next poll doesn't start over from the first page.
    last_full, empty = pages.last(2)
    assert_equal last_full.next_cursor, empty.next_cursor

    # Fix the data, then resume: the resume recomputes the column from the
    # source rows as they are now.
    pg.exec("update ledger set n = id")

    applied = Trellis.apply("RESUME TRANSFORM doubled_ledger.doubled")
    assert_equal :resumed, applied.kind
    assert_equal ["doubled_ledger.doubled"], applied.columns

    assert_equal Trellis::QuarantineEntry.new(target: "doubled_ledger.doubled", state: :live,
                                              paused_at: nil, last_error: nil),
                 Trellis.quarantine_status("doubled_ledger.doubled")
    assert_equal [], Trellis.quarantined
    assert_equal [], Trellis.sample_quarantined("doubled_ledger.doubled").samples

    fixed = [[1, 2], [2, 4], [3, 6], [4, 8], [5, 10]]
    eventually_value("doubled_ledger to hold the fixed rows", ->(rows) { rows == fixed }) do
      pg.exec("select id, doubled from doubled_ledger order by id").values
        .map { |row| row.map(&:to_i) }
    end
  ensure
    pg&.close
  end

  # A failure the column fuse can't pin on one column (here, the target
  # table's own check constraint refusing the write) poisons the whole key,
  # which poisoned_since reports and the transform's own address samples.
  #
  # No public call releases a poisoned key, and a held one stops
  # await_converged converging past it (#588), so this runs on a cluster of
  # its own rather than wedging the shared one.
  def test_a_poisoned_key_is_reported_by_poisoned_since_and_sampled_under_its_transform
    TestCluster.private_cluster do |cluster|
      dsn = cluster.fetch("dsn")
      pg = TestCluster.pg(cluster)
      pg.exec("create table gizmos (id integer primary key, price integer)")

      Trellis.connect(url: dsn)
      Trellis.migrate
      Trellis.shutdown

      begin
        Trellis.connect(url: dsn, staging: true, drain_threads: 1)
        Trellis.apply("TRANSFORM gizmo_prices FROM gizmos SELECT price AS price")
        await_status("gizmo_prices")
        pg.exec("alter table gizmo_prices add constraint cheap check (price < 100)")

        before = Time.now
        pg.exec("insert into gizmos (id, price) values (1, 5), (2, 500)")

        entries = eventually_value("gizmos key 2 to be poisoned", ->(seen) { !seen.empty? }) do
          Trellis.poisoned_since(before)
        end
        assert_equal 1, entries.length
        entry = entries.first
        assert_instance_of Trellis::PoisonEntry, entry
        assert_equal ["public.gizmos", "2"], [entry.src_table, entry.key]
        assert_match "cheap", entry.last_error
        assert_instance_of Time, entry.poisoned_at
        assert_operator entry.poisoned_at, :>, before

        # The watermark is exclusive: nothing was poisoned after the entry
        # itself, and its Time round-trips to the microsecond.
        assert_equal [], Trellis.poisoned_since(entry.poisoned_at)

        page = Trellis.sample_quarantined("gizmo_prices")
        assert_equal [["public.gizmos", "2"]], page.samples.map { |s| [s.src_table, s.key] }

        # One poisoned key is below the whole-transform fuse: the transform
        # stays live, and the good row goes through.
        assert_equal :live, Trellis.quarantine_status("gizmo_prices").state

        # The eviction commits on its own, and only then does the drain retry
        # the batch without key 2, so the good row can land a moment after
        # the poison entry is visible.
        eventually_value("gizmo_prices to hold the good row", ->(rows) { rows == [%w[1 5]] }) do
          pg.exec("select id, price from gizmo_prices").values
        end
      ensure
        # Before the private cluster goes away under it.
        Trellis.shutdown
        pg.close
      end
    end
  end

  def test_the_quarantine_calls_refuse_malformed_arguments_before_calling_the_engine
    Trellis.connect(url: TestCluster.dsn)

    assert_raises(Trellis::ValidationError) do
      Trellis.sample_quarantined("t.c", after: "not a cursor")
    end
    assert_raises(Trellis::ValidationError) { Trellis.sample_quarantined("t.c", after: 5) }
    [0, -1, 1.5, 2**63, nil].each do |limit|
      error = assert_raises(Trellis::ValidationError, limit.inspect) do
        Trellis.sample_quarantined("t.c", limit:)
      end
      assert_match "limit", error.message
    end
    assert_raises(ArgumentError) { Trellis.sample_quarantined("t.c", page: 2) }

    assert_raises(Trellis::ValidationError) { Trellis.poisoned_since("yesterday") }
    assert_raises(Trellis::ValidationError) { Trellis.poisoned_since(Time.at(2**62)) }
    assert_raises(Trellis::ValidationError) { Trellis.quarantine_status(:t) }
  end

  private

  # Every page of target's sample, `limit` rows at a time, through to the
  # first empty page. Bounded, so a cursor that never advances fails rather
  # than looping.
  def sample_pages(target, limit)
    pages = []
    cursor = nil
    20.times do
      page = Trellis.sample_quarantined(target, limit:, after: cursor)
      assert_instance_of Trellis::SamplePage, page
      pages << page
      return pages if page.samples.empty?

      cursor = page.next_cursor
    end
    flunk "sampling #{target} never reached an empty page; last saw #{pages.last.inspect}"
  end
end
