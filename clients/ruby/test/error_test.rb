# frozen_string_literal: true

require "test_helper"

class ErrorTest < Minitest::Test
  # ADR-0010 decision 4: every code the engine reports today has its own
  # subclass, so a new code is a deliberate change here rather than a silent
  # downgrade to UnknownError. `trellis-embed`'s own test fails when the
  # engine grows a code its list (Trellis::Native.error_codes) lacks.
  def test_every_error_code_the_engine_reports_maps_to_its_own_class
    codes = Trellis::Native.error_codes
    refute_empty codes

    codes.each do |code|
      error = Trellis::Error.from_native(code, "boom")
      refute_instance_of Trellis::UnknownError, error,
                         "error code #{code.inspect} has no Trellis::Error subclass; add it"
      assert_equal code.to_sym, error.code
      assert_equal "boom", error.message
    end
  end

  # And the other way: the map names no code the engine has dropped.
  def test_the_mapping_names_exactly_the_engines_codes
    assert_equal Trellis::Native.error_codes.sort, Trellis::Error::BY_CODE.keys.sort
  end

  def test_a_code_this_binding_does_not_know_becomes_an_unknown_error_naming_it
    error = Trellis::Error.from_native("brand_new_code", "something broke")
    assert_instance_of Trellis::UnknownError, error
    assert_equal :unknown, error.code
    assert_match "brand_new_code", error.message
    assert_match "something broke", error.message
  end

  def test_every_error_is_a_trellis_error_and_a_standard_error
    Trellis::Error::BY_CODE.each_value do |klass|
      assert_operator klass, :<, Trellis::Error
    end
    assert_operator Trellis::Error, :<, StandardError
  end

  def test_the_fork_error_is_a_validation_error
    error = Trellis::ForkedHandleError.new("forked")
    assert_kind_of Trellis::ValidationError, error
    assert_equal :validation, error.code
  end

  def test_status_symbols_come_from_a_closed_set
    names = Trellis::Native.status_names
    assert_includes names, :live
    assert_includes names, :waiting_to_backfill
    assert(names.all?(Symbol))
  end
end
