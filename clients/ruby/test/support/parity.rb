# frozen_string_literal: true

require "json"

# The Ruby half of the cross-language parity suite (issue #155): runs
# clients/parity/cases.json, which the Elixir suite runs too, against this
# binding. The shape language and the canonical form are described in
# clients/parity/README.md; test/trellis/parity_test.exs is the Elixir
# counterpart of this file, and the two must read the fixture the same way.
module Parity
  FIXTURE_PATH = File.expand_path("../../../parity/cases.json", __dir__)

  # The closed word sets a `word_in` shape names, each read from the native
  # extension (so from trellis-embed), never written out here.
  WORD_SETS = {
    "transform_status" => -> { Trellis::Native.status_names },
    "quarantine_state" => -> { Trellis::Native.quarantine_states },
    "cardinality" => -> { Trellis::Native.cardinality_names },
    "applied_kind" => -> { Trellis::Native.applied_kinds },
    "self_check_outcome" => -> { Trellis::Native.self_check_outcomes },
    "divergence_kind" => -> { Trellis::Native.divergence_kinds },
    "capture_failure_kind" => -> { Trellis::Native.capture_failure_kinds }
  }.freeze

  # The fixture's operations, each as a call on this binding. `args` are the
  # step's positional arguments and `opts` its options, both already turned
  # into Ruby values.
  OPERATIONS = {
    "connect" => ->(_args, opts) { Trellis.connect(**opts) },
    "migrate" => ->(_args, _opts) { Trellis.migrate },
    "config" => ->(_args, _opts) { Trellis.config },
    "define" => ->(args, _opts) { Trellis.define(*args) },
    "apply" => ->(args, _opts) { Trellis.apply(*args) },
    "status" => ->(args, _opts) { Trellis.status(*args) },
    "definitions" => ->(_args, _opts) { Trellis.definitions },
    "relationships" => ->(_args, _opts) { Trellis.relationships },
    "request_backfill" => ->(args, _opts) { Trellis.request_backfill(*args) },
    "release_key" => ->(args, _opts) { Trellis.release_key(*args) },
    "poisoned_since" => ->(args, _opts) { Trellis.poisoned_since(*args) },
    "quarantined" => ->(_args, _opts) { Trellis.quarantined },
    "quarantine_status" => ->(args, _opts) { Trellis.quarantine_status(*args) },
    "sample_quarantined" => ->(args, opts) { Trellis.sample_quarantined(*args, **opts) },
    "has_live_drain_workers" => ->(_args, _opts) { Trellis.has_live_drain_workers? },
    "has_live_staging_worker" => ->(_args, _opts) { Trellis.has_live_staging_worker? },
    "watermark_token" => ->(_args, _opts) { Trellis.watermark_token },
    "await_converged" => ->(args, opts) { Trellis.await_converged(*args, **opts) },
    "self_check" => ->(args, opts) { Trellis.self_check(*args, **opts) },
    "shutdown" => ->(_args, _opts) { Trellis.shutdown }
  }.freeze

  # The public functions with no fixture operation, and why.
  NOT_OPERATIONS = {
    # The handle lives in the module here and in a variable in Elixir, so
    # only Ruby has a call that asks whether there is one.
    "connected?" => "Ruby's module-held handle"
  }.freeze

  # The Rails integration's public calls outside the migration helpers, none
  # of them on `Trellis` itself, and why each has no fixture operation. (Each
  # migration helper, Trellis::Migration#define, #apply and #status, is the
  # fixture operation of the same name; test/migration_test.rb runs them.)
  # Elixir lists its supervision calls, child_spec and start_link, the same
  # way.
  RAILS_ONLY = {
    "Trellis::Railtie.connect" => "`connect` with config.trellis.connect's options",
    "Trellis::Railtie.connect_options" => "reads config.trellis.connect",
    "Trellis::Railtie.with_handle" => "a handle of its own from config.trellis.connect, around a block",
    "Trellis::Migration.with_handle" => "Trellis::Railtie.with_handle, raising a migration's error"
  }.freeze

  # The fixture's own steps, which drive the test rather than the binding.
  HARNESS_OPS = %w[sql now].freeze

  # How long an `until` step polls before failing (#297).
  UNTIL_SECONDS = 30

  def self.fixture
    @fixture ||= JSON.parse(File.read(FIXTURE_PATH))
  end

  # A host value in the fixture's canonical form: nil, true/false, Integer,
  # String and Array as themselves; a symbol as {"$word" => name}; a Time
  # as {"$time" => epoch microseconds}; a record as {"$record" => name,
  # "fields" => {...}}; a Hash as {"$map" => {...}}; a Trellis::Error as
  # {"$error" => code}. Checks the Ruby-side conventions on the way (UTC
  # microsecond times, string map keys, the error class a code maps to) and
  # raises Mismatch when one is broken.
  def self.canonical(value)
    case value
    when nil, true, false, Integer then value
    when String
      raise Mismatch, "string #{value.inspect} isn't valid UTF-8" unless value.valid_encoding? &&
                                                                          value.encoding == Encoding::UTF_8

      value
    when Symbol then { "$word" => value.to_s }
    when ::Time then { "$time" => time_micros(value) }
    when Array then value.map { |item| canonical(item) }
    when Data then record(value)
    when Hash then { "$map" => value.to_h { |key, item| [map_key(key), canonical(item)] } }
    when Trellis::Error then { "$error" => error_code(value) }
    else raise Mismatch, "#{value.inspect} (a #{value.class}) has no canonical form"
    end
  end

  def self.time_micros(time)
    raise Mismatch, "time #{time.inspect} isn't UTC" unless time.utc?
    raise Mismatch, "time #{time.inspect} is finer than a microsecond" unless (time.nsec % 1_000).zero?

    (time.to_r * 1_000_000).to_i
  end

  def self.record(value)
    name = value.class.name.to_s
    raise Mismatch, "#{value.inspect} isn't a Trellis record" unless name.start_with?("Trellis::")

    fields = value.to_h.to_h { |field, item| [field.to_s, canonical(item)] }
    { "$record" => name.delete_prefix("Trellis::"), "fields" => fields }
  end

  def self.map_key(key)
    raise Mismatch, "map key #{key.inspect} isn't a String" unless key.is_a?(String)

    key
  end

  # The error's code, once its class is the one that code maps to:
  # "not_found" is a Trellis::NotFoundError, and a code this binding doesn't
  # know an UnknownError.
  def self.error_code(error)
    code = error.code.to_s
    expected = "Trellis::#{code.split('_').map(&:capitalize).join}Error"
    unless error.instance_of?(Object.const_get(expected))
      raise Mismatch, "a #{code.inspect} error is a #{error.class}, not a #{expected}"
    end
    raise Mismatch, "a #{code.inspect} error's message isn't a String" unless error.message.is_a?(String)

    code
  end

  class Mismatch < StandardError; end

  # Checks `value` (canonical) against `shape`, and returns every mismatch
  # as "path: problem" (empty when it matches).
  class Matcher
    def initialize(saved)
      @saved = saved
      @records = Parity.fixture.fetch("records")
    end

    def mismatches(shape, value, path = "result")
      problems = []
      check(shape, value, path, problems)
      problems
    end

    private

    def check(shape, value, path, problems)
      case shape
      when nil
        problems << "#{path}: expected nil, got #{value.inspect}" unless value.nil?
      when String then check_type(shape, value, path, problems)
      when Hash then check_form(shape, value, path, problems)
      else raise ArgumentError, "#{path}: the fixture has no shape #{shape.inspect}"
      end
    end

    def check_type(type, value, path, problems)
      ok = case type
           when "string" then value.is_a?(String)
           when "integer" then value.is_a?(Integer)
           when "boolean" then [true, false].include?(value)
           when "time" then value.is_a?(Hash) && value.key?("$time")
           else raise ArgumentError, "#{path}: the fixture has no type #{type.inspect}"
           end
      problems << "#{path}: expected a #{type}, got #{value.inspect}" unless ok
    end

    def check_form(shape, value, path, problems)
      form = (shape.keys - ["fields"]).first
      if shape.size != (shape.key?("fields") ? 2 : 1) || (shape.key?("fields") && form != "record")
        raise ArgumentError, "#{path}: the fixture has no shape #{shape.inspect}"
      end

      arg = shape.fetch(form)
      case form
      when "eq" then expect_equal(arg, value, path, problems)
      when "word" then expect_equal({ "$word" => arg }, value, path, problems)
      when "word_in" then check_word_in(arg, value, path, problems)
      when "time_micros" then expect_equal({ "$time" => arg }, value, path, problems)
      when "error" then expect_equal({ "$error" => arg }, value, path, problems)
      when "ref" then expect_equal(Parity.canonical(Parity.resolve(@saved, arg)), value, path, problems)
      when "nullable" then check(arg, value, path, problems) unless value.nil?
      when "list", "list_of" then check_list(form, arg, value, path, problems)
      when "map", "map_of" then check_map(form, arg, value, path, problems)
      when "record" then check_record(arg, shape.fetch("fields", {}), value, path, problems)
      else raise ArgumentError, "#{path}: the fixture has no shape #{shape.inspect}"
      end
    end

    def expect_equal(expected, value, path, problems)
      problems << "#{path}: expected #{expected.inspect}, got #{value.inspect}" unless expected == value
    end

    def check_word_in(set, value, path, problems)
      words = Parity::WORD_SETS.fetch(set) { raise ArgumentError, "#{path}: no word set #{set}" }
                               .call.map(&:to_s)
      return if value.is_a?(Hash) && words.include?(value["$word"])

      problems << "#{path}: expected a word from #{set} #{words.inspect}, got #{value.inspect}"
    end

    def check_list(form, arg, value, path, problems)
      return problems << "#{path}: expected a list, got #{value.inspect}" unless value.is_a?(Array)

      if form == "list" && arg.length != value.length
        return problems << "#{path}: expected #{arg.length} items, got #{value.length}: #{value.inspect}"
      end

      value.each_with_index do |item, i|
        check(form == "list" ? arg[i] : arg, item, "#{path}[#{i}]", problems)
      end
    end

    def check_map(form, arg, value, path, problems)
      map = value.is_a?(Hash) && value["$map"]
      return problems << "#{path}: expected a map, got #{value.inspect}" unless map

      if form == "map" && arg.keys.sort != map.keys.sort
        return problems << "#{path}: expected keys #{arg.keys.sort.inspect}, got #{map.keys.sort.inspect}"
      end

      map.each { |key, item| check(form == "map" ? arg.fetch(key) : arg, item, "#{path}[#{key.inspect}]", problems) }
    end

    def check_record(name, overrides, value, path, problems)
      schema = @records.fetch(name) { raise ArgumentError, "#{path}: no record #{name} in the fixture" }
      unless value.is_a?(Hash) && value["$record"] == name
        return problems << "#{path}: expected a #{name}, got #{value.inspect}"
      end

      fields = value.fetch("fields")
      unless fields.keys.sort == schema.keys.sort
        return problems << "#{path}: #{name}'s fields are #{fields.keys.sort.inspect}, " \
                           "expected #{schema.keys.sort.inspect}"
      end

      unknown = overrides.keys - schema.keys
      raise ArgumentError, "#{path}: #{name} has no fields #{unknown.inspect}" unless unknown.empty?

      schema.each do |field, field_shape|
        check(overrides.fetch(field, field_shape), fields.fetch(field), "#{path}.#{field}", problems)
      end
    end
  end

  # A saved host value, then down `path` ("page_1.next_cursor",
  # "poisoned.0.poisoned_at").
  def self.resolve(saved, path)
    name, *rest = path.split(".")
    value = saved.fetch(name) { raise ArgumentError, "nothing was saved as #{name.inspect}" }
    rest.reduce(value) do |current, segment|
      case current
      when Array then current.fetch(Integer(segment))
      when Data then current.public_send(segment)
      else raise ArgumentError, "can't follow #{segment.inspect} into #{current.inspect}"
      end
    end
  end

  # A fixture argument as a Ruby value: {"$ref"}, {"$word"}, {"$time_micros"}
  # and {"$dsn"} are resolved; anything else is taken as it is.
  def self.argument(arg, saved:, cluster:)
    case arg
    when Array then arg.map { |item| argument(item, saved:, cluster:) }
    when Hash
      if arg.key?("$ref") then resolve(saved, arg.fetch("$ref"))
      elsif arg.key?("$word") then arg.fetch("$word").to_sym
      elsif arg.key?("$time_micros") then Trellis::EpochMicros.to_time(arg.fetch("$time_micros"))
      elsif arg.key?("$dsn") then dsn(cluster.fetch("dsn"), arg.fetch("$dsn"))
      else arg.to_h { |key, item| [key.to_sym, argument(item, saved:, cluster:)] }
      end
    else arg
    end
  end

  # `dsn` with each of `overrides` ({"user" => "..."}) set in place of its
  # own.
  def self.dsn(dsn, overrides)
    overrides.reduce(dsn) do |current, (key, value)|
      pattern = /(?<=\A|\s)#{Regexp.escape(key)}=\S*/
      current.match?(pattern) ? current.sub(pattern, "#{key}=#{value}") : "#{current} #{key}=#{value}"
    end
  end

  # A native map from the fixture's conversions, as the extension would hand
  # it over: symbol keys, {"$word"} as a symbol.
  def self.native(value)
    case value
    when Array then value.map { |item| native(item) }
    when Hash
      return value.fetch("$word").to_sym if value.key?("$word")

      value.to_h { |key, item| [key.to_sym, native(item)] }
    else value
    end
  end

  # Every shape anywhere under `shape`, for the coverage checks.
  def self.each_shape(shape, &block)
    return enum_for(:each_shape, shape) unless block

    yield shape
    case shape
    when Array then shape.each { |item| each_shape(item, &block) }
    when Hash then shape.each_value { |item| each_shape(item, &block) }
    end
  end
end
