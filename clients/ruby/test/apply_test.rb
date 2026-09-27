# frozen_string_literal: true

require "test_helper"

# Trellis.apply against a running engine, once per statement form, checking
# the Applied each one returns, plus the reads and operational calls that
# sit alongside it. Mirrors the Elixir suite's apply_test.exs.
class ApplyTest < Minitest::Test
  include TrellisTestCase

  def setup
    super
    @pg = TestCluster.pg
    Trellis.connect(url: TestCluster.dsn, staging: true, drain_threads: 1)
  end

  def teardown
    @pg&.close
    super
  end

  def test_every_statement_form_round_trips_through_apply
    @pg.exec("create table owners (id integer primary key, name text)")
    @pg.exec("create table pets (id integer primary key, owner_id integer, weight integer)")
    # A relationship re-derives from each side's old row images.
    @pg.exec("alter table owners replica identity full")
    @pg.exec("alter table pets replica identity full")
    @pg.exec("insert into pets (id, owner_id, weight) values (1, 1, 4)")

    # Keyed on each kind's `trellis::StatementKind` name, so a statement form
    # the engine adds fails the last assertion until it is covered here.
    covered = []

    # `owners.id` is the primary key, so each pet has at most one owner. No
    # index on `pets.owner_id` comes back as a warning, not an error.
    applied = Trellis.apply("RELATIONSHIP owner FROM pets.owner_id TO owners.id")
    assert_equal :relationship_defined, applied.kind
    relationship = applied.relationship
    assert_instance_of Trellis::Relationship, relationship
    assert_equal ["owner", "public", "pets", "owner_id", "public", "owners", "id", :one],
                 relationship.to_h.values_at(:name, :from_schema, :from_table, :from_col,
                                             :to_schema, :to_table, :to_col, :cardinality)
    assert_equal 1, relationship.warnings.length
    assert_match "pets", relationship.warnings.first
    assert_nil applied.definition
    covered << "define_relationship"

    summary = Trellis.relationships.find { |r| r.id == relationship.id }
    assert_instance_of Trellis::RelationshipSummary, summary
    assert_equal "owner", summary.name
    assert_equal :one, summary.cardinality
    assert_instance_of Time, summary.created_at
    assert_in_delta Time.now, summary.created_at, 60

    applied = Trellis.apply("TRANSFORM pet_weights FROM pets SELECT weight AS weight")
    assert_equal :transform_defined, applied.kind
    definition = applied.definition
    assert_instance_of Trellis::Definition, definition
    assert_equal "public.pet_weights", definition.target_table
    assert_equal "public.pets", definition.source_table
    assert_equal :waiting_to_backfill, definition.status
    covered << "define_transform"

    summary = Trellis.definitions.find { |d| d.id == definition.id }
    assert_instance_of Trellis::DefinitionSummary, summary
    assert_equal "public.pet_weights", summary.target_table
    assert_instance_of Time, summary.created_at
    assert_nil summary.backfill_failure

    await_status("pet_weights")

    applied = Trellis.apply("ALTER TRANSFORM pet_weights ADD weight + weight AS doubled")
    assert_equal :altered, applied.kind
    assert_equal "public.pet_weights", applied.definition.target_table
    assert_equal [["doubled"], [], []], [applied.added, applied.dropped, applied.altered]
    covered << "alter_transform"

    assert_equal Trellis::Applied.new(kind: :paused, definition: nil, relationship: nil,
                                      columns: nil, added: nil, dropped: nil, altered: nil),
                 Trellis.apply("PAUSE TRANSFORM pet_weights")
    assert_equal :paused, Trellis.status("pet_weights").status
    covered << "pause_transform"

    # A whole-transform resume has no columns to report, and rebuilds.
    applied = Trellis.apply("RESUME TRANSFORM pet_weights")
    assert_equal :resumed, applied.kind
    assert_equal [], applied.columns
    covered << "resume_transform"
    await_status("pet_weights")

    # A definition is paused before it is dropped. Pausing a paused one is
    # the same success.
    assert_equal :paused, Trellis.apply("PAUSE TRANSFORM pet_weights").kind
    assert_equal :paused, Trellis.apply("PAUSE TRANSFORM pet_weights").kind
    assert_equal :dropped, Trellis.apply("DROP TRANSFORM pet_weights").kind
    assert_nil Trellis.status("pet_weights")
    covered << "drop_transform"

    assert_equal :dropped, Trellis.apply("DROP RELATIONSHIP pets.owner").kind
    refute(Trellis.relationships.any? { |r| r.id == relationship.id })
    covered << "drop_relationship"

    assert_equal Trellis::Native.statement_kinds.sort, covered.sort
  end

  def test_a_statement_apply_cannot_carry_out_raises
    error = assert_raises(Trellis::ParseError) { Trellis.apply("TRANSFORM oops") }
    assert_equal :parse, error.code
    assert_raises(Trellis::NotFoundError) { Trellis.apply("PAUSE TRANSFORM no_such_transform") }
    assert_raises(Trellis::ValidationError) { Trellis.apply(:not_a_string) }
  end

  def test_request_backfill_rereads_a_captured_table_and_refuses_an_unknown_one
    @pg.exec("create table crates (id integer primary key, size integer)")
    Trellis.apply("TRANSFORM crate_sizes FROM crates SELECT size AS size")
    await_status("crate_sizes")

    assert_nil Trellis.request_backfill("crates")
    await_status("crate_sizes")

    assert_raises(Trellis::NotFoundError) { Trellis.request_backfill("no_such_table") }
  end

  def test_a_running_handle_reports_live_workers
    assert_equal true, eventually_value("a live staging worker", ->(live) { live }) {
      Trellis.has_live_staging_worker?
    }
    assert_equal true, eventually_value("a live drain worker", ->(live) { live }) {
      Trellis.has_live_drain_workers?
    }
  end

  def test_await_converged_waits_out_a_write_by_a_token_read_back_unchanged
    @pg.exec("create table bolts (id integer primary key, length integer)")
    Trellis.apply("TRANSFORM bolt_lengths FROM bolts SELECT length AS length")
    await_status("bolt_lengths")

    @pg.exec("insert into bolts (id, length) values (1, 30)")
    token = Trellis.watermark_token
    assert_instance_of String, token
    assert_nil Trellis.await_converged(token, timeout_ms: 30_000)

    # No polling: the write has reached the target once the call returns.
    assert_equal [%w[1 30]], @pg.exec("select id, length from bolt_lengths").values

    assert_raises(Trellis::ValidationError) do
      Trellis.await_converged("not a token", timeout_ms: 1_000)
    end
    [-1, 2**64, 1.5, nil].each do |timeout_ms|
      error = assert_raises(Trellis::ValidationError, timeout_ms.inspect) do
        Trellis.await_converged(token, timeout_ms:)
      end
      assert_match "timeout_ms", error.message
    end
    assert_raises(ArgumentError) { Trellis.await_converged(token) }
  end
end
