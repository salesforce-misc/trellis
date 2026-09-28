# frozen_string_literal: true

# Checks an installed trellis-pg gem loads its extension and calls into it.
# Run by .github/workflows/ruby-release.yml against every gem it builds,
# installed on its own (not from a checkout).
#
# usage: ruby ruby-gem-smoke.rb [<dsn>]
#
# With a libpq DSN (a `trellis-testkit` cluster's, say), it connects,
# migrates and reads a status. Without one, it points a handle at a port
# nothing listens on and expects the read to fail as a
# Trellis::ConnectivityError: the extension loaded, started its runtime and
# reported the failure through the error mapping.
require "trellis/pg"

spec = Gem.loaded_specs.fetch("trellis-pg") do
  abort "trellis/pg didn't load from an installed trellis-pg gem: #{$LOADED_FEATURES.grep(/trellis/).inspect}"
end
extension = $LOADED_FEATURES.find { |f| f.include?("trellis_ruby") }
puts "trellis-pg #{Trellis::VERSION} (#{spec.platform}) on Ruby #{RUBY_VERSION} (#{RUBY_PLATFORM}): #{extension}"

dsn = ARGV[0]
if dsn
  Trellis.connect(url: dsn)
  Trellis.migrate
  status = Trellis.status("no_such_target")
  abort "status of an unknown target: #{status.inspect}, not nil" unless status.nil?
  puts "connected, migrated, #{Trellis.definitions.size} definitions"
else
  Trellis.connect(url: "host=127.0.0.1 port=1 dbname=none connect_timeout=5")
  begin
    Trellis.status("no_such_target")
    abort "a status read with no server behind it didn't fail"
  rescue Trellis::ConnectivityError => e
    puts "no server, as expected: #{e.message}"
  end
end
Trellis.shutdown
