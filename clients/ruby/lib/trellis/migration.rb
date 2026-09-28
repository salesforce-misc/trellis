# frozen_string_literal: true

require "active_record"
require "trellis"

module Trellis
  # Trellis statements in an ActiveRecord migration, so a transform is
  # defined, and dropped, the way the tables it reads are created.
  #
  #   class DefineWidgetPrices < ActiveRecord::Migration[8.1]
  #     include Trellis::Migration
  #
  #     # Required: see "Transactions" below.
  #     disable_ddl_transaction!
  #
  #     def up
  #       create_table :widgets do |t|
  #         t.integer :price
  #       end
  #
  #       define "TRANSFORM widget_prices FROM widgets SELECT price AS price"
  #     end
  #
  #     def down
  #       apply "PAUSE TRANSFORM widget_prices"
  #       apply "DROP TRANSFORM widget_prices"
  #       drop_table :widgets
  #     end
  #   end
  #
  # Three helpers, each a thin call through to the Trellis method of the
  # same name:
  #
  # - define: a `TRANSFORM` statement, and nothing else (Trellis.define).
  # - apply: any statement of the grammar (Trellis.apply), such as the
  #   `PAUSE TRANSFORM` and `DROP TRANSFORM` a `down` undoes a define with.
  #   (`DROP` refuses a transform that isn't paused, and drops its target
  #   table.)
  # - status: the Status of the transform writing a target, or nil
  #   (Trellis.status).
  #
  # Each runs as it's called, in order with the migration's own statements,
  # so a transform can read a table created earlier in the same migration.
  # Each raises the Trellis::Error if its call fails, which fails the
  # migration. (ActiveRecord's migrator re-raises it as a StandardError
  # naming the migration, with the Trellis::Error as its cause.)
  #
  # == Transactions
  #
  # Trellis doesn't join the migration's transaction. It runs every call on
  # connections of its own, and each call commits before it returns. If the
  # migration's transaction rolled back after a define, the host's changes
  # would be undone and the definition and its target table would not.
  #
  # So a migration that includes this module has to turn its transaction off
  # with `disable_ddl_transaction!`. define and apply check, as they run,
  # that the migration's connection has no transaction open (a `transaction
  # do` block inside the migration opens one too), and raise
  # ActiveRecord::MigrationError before applying anything if it does. In a
  # migration that forgot `disable_ddl_transaction!`, the migration's
  # transaction then rolls back with nothing applied on either side.
  #
  # Without the transaction, a migration that fails partway leaves whatever
  # ran before the failure in place, on both sides. Give a migration that
  # defines transforms little else to do.
  #
  # == up and down, not change
  #
  # ActiveRecord can reverse `create_table` on its own, but not a Trellis
  # statement: the reverse of a define is a pause and a drop of a target
  # that only the statement names. So a migration that includes this module
  # must define `up` and `down`. One that defines a public `change` raises
  # ActiveRecord::MigrationError before running anything, in either
  # direction, rather than roll back partway. define and apply inside a
  # `revert` block raise ActiveRecord::IrreversibleMigration.
  #
  # == Which Trellis
  #
  # If this process's handle is connected, the helpers use it. Otherwise
  # (the usual case: `rails db:migrate` gets no boot handle, see
  # Trellis::Railtie) each call connects a handle of its own from
  # config.trellis.connect, runs Trellis.migrate on it (creating Trellis's
  # own tables the first time), makes its one call, and shuts the handle
  # down. A migration's own handle never runs background work: staging and
  # drain_threads are always the defaults, whatever the configuration says.
  #
  # == Running a migration once
  #
  # A define isn't idempotent: a second define of a target that exists
  # raises ConflictError. ActiveRecord's schema_migrations already records
  # which migrations have run, so a migration's define runs once per
  # database, and these helpers add no guard of their own. A migration that
  # has to tolerate a transform defined some other way (by hand, or by a
  # deploy step) can guard the define itself:
  #
  #   def up
  #     define "TRANSFORM widget_prices FROM widgets SELECT price AS price" if status("widget_prices").nil?
  #   end
  #
  # The guard checks the target's name, not its definition: a changed
  # statement for a target that exists is an `ALTER TRANSFORM`.
  module Migration
    # The helpers' Trellis::Railtie.with_handle: runs the block with this
    # process's handle, or one connected from config.trellis.connect for the
    # length of the block, running Trellis.migrate first either way, and
    # returns the block's value. Without Rails, it uses this process's
    # handle. Raises ActiveRecord::MigrationError if there's neither.
    def self.with_handle(&)
      if Trellis.connected?
        Trellis.migrate
        return yield
      end

      unless defined?(Trellis::Railtie) && Trellis::Railtie.connect_options
        raise ActiveRecord::MigrationError,
              "there's no Trellis to run this against: set config.trellis.connect to " \
              "Trellis.connect's options (config.trellis.connect = { url: ENV.fetch(\"DATABASE_URL\") }), " \
              "or call Trellis.connect before migrating"
      end

      Trellis::Railtie.with_handle(&)
    end

    # Registers a transform from a `TRANSFORM` statement and returns its
    # Definition: Trellis.define, run as the migration reaches it.
    def define(statement)
      trellis_statement("define", statement) { Trellis.define(statement) }
    end

    # Runs any statement of Trellis's grammar and returns its Applied:
    # Trellis.apply, run as the migration reaches it.
    def apply(statement)
      trellis_statement("apply", statement) { Trellis.apply(statement) }
    end

    # The Status of the transform writing target_table, or nil when none
    # does: Trellis.status. A read, so it runs whether or not a transaction
    # is open.
    def status(target_table)
      Migration.with_handle { Trellis.status(target_table) }
    end

    # ActiveRecord's entry point for running this migration in a direction:
    # refuses a change method before anything runs.
    def exec_migration(conn, direction)
      if respond_to?(:change)
        raise ActiveRecord::MigrationError,
              "#{self.class.name} includes Trellis::Migration, so it must define up and down " \
              "rather than change: ActiveRecord can't reverse a Trellis statement. Undo a " \
              "define in down with apply \"PAUSE TRANSFORM <target>\" and " \
              "apply \"DROP TRANSFORM <target>\"."
      end
      super
    end

    private

    def trellis_statement(helper, statement)
      if reverting?
        raise ActiveRecord::IrreversibleMigration,
              "cannot reverse a Trellis statement: #{statement}. Undo a define in down with " \
              "PAUSE TRANSFORM and DROP TRANSFORM statements."
      end
      refuse_open_transaction!("this statement wasn't applied: #{statement}")

      say_with_time("#{helper}(#{statement.inspect})") { Migration.with_handle { yield } }
    end

    def refuse_open_transaction!(what)
      return unless connection.transaction_open?

      raise ActiveRecord::MigrationError,
            "Trellis doesn't join the migration's transaction, so #{what}: if the " \
            "transaction rolled back, a definition would stay. Add `disable_ddl_transaction!` " \
            "to the migration, and keep Trellis statements out of `transaction` blocks."
    end
  end
end
