# frozen_string_literal: true

# A Rails app in the test process, never initialized, so the Rails
# integration has a config.trellis to read. test_helper required "trellis"
# before Rails was loaded, as an app whose Gemfile lists trellis ahead of
# rails would, so the Railtie is required here by hand.
require "logger"
require "rails"
require "active_record"
require "trellis/railtie"
require "trellis/migration"

class TrellisTestApp < Rails::Application
  config.root = File.expand_path("../..", __dir__)
  config.eager_load = false
  config.logger = Logger.new(nil)
end

ActiveRecord::Migration.verbose = false
