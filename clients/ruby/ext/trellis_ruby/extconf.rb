# frozen_string_literal: true

# Builds the native extension through Cargo with rb-sys: what installing the
# source gem runs, and what `rake compile` and the cross-compiled platform
# gems run too (see the Rakefile).
#
# The `ruby` feature is the whole extension (see Cargo.toml). The `trellis`
# crate's optional `otlp` feature stays off: nothing in the binding uses it,
# and it would pull in an OpenTelemetry and gRPC dependency tree.
require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("trellis/trellis_ruby") do |r|
  r.features = ["ruby"]
end
