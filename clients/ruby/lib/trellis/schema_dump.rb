# frozen_string_literal: true

require "active_record"
require "trellis/pg"

module Trellis
  # Makes `structure.sql` carry Trellis's definitions, the way Rails makes it
  # carry `schema_migrations`: after `pg_dump` writes the schema, the dump
  # appends the `INSERT`s that describe how the schema came to be. A database
  # built from the file (`db:schema:load`, `db:prepare`, the test database
  # `maintain_test_schema!` loads) then has every transform and relationship
  # defined, with no Trellis step to run: Trellis's worker finds the
  # definitions unbuilt and builds them (issue #644, ADR-0010).
  #
  # Needs `config.active_record.schema_format = :sql`: a `schema.rb` can't
  # carry another schema's tables or triggers. With `:ruby`, the dump raises
  # Trellis::ValidationError and writes nothing: a `schema.rb` would load
  # into a database with target tables and no definitions.
  #
  # It hooks the two places Rails offers no extension point for (both the
  # `db:schema:dump` task and the dump after `db:migrate` call them):
  #
  # - ActiveRecord::Tasks::DatabaseTasks.dump_schema, whose `:sql` branch
  #   is `pg_dump` followed by the `schema_migrations` rows. This hook adds
  #   the Trellis rows after them: Trellis::Instance#dump_definitions_sql
  #   for the default instance and each of config.trellis.instances. Those
  #   rows are read before and after the `pg_dump`, which reads its own
  #   snapshot, and the dump is redone (up to three times) when a define or
  #   drop committed in between, so the DDL and the rows agree.
  # - ActiveRecord::Tasks::PostgreSQLDatabaseTasks#structure_dump, which
  #   passes `--schema` only for the search path's schemas. This hook adds
  #   each instance's `schema` and `target_schema`, so the Trellis tables and
  #   capture triggers are in the dump. (Nothing is added when Rails dumps
  #   every schema anyway, `ActiveRecord.dump_schemas = :all` or no
  #   `schema_search_path`.)
  #
  # Both apply to one database: the config named by
  # `config.trellis.dump_database` (default "primary"), the one
  # config.trellis.connect and config.trellis.instances point at. The other
  # databases of a multi-database app dump as Rails dumps them.
  #
  # A dump lists definitions and nothing else of Trellis's state. Every
  # definition loads as `waiting_to_backfill`, as a `define` leaves it,
  # including one that was paused or quarantined where the dump was taken.
  #
  # `pg_dump` 17 writes `CREATE SCHEMA public` when `public` is dumped,
  # which fails to load into a database that has one (every database
  # `db:create` makes); the hook writes `CREATE SCHEMA IF NOT EXISTS public`.
  module SchemaDump
    # How many times a dump is redone when a define or drop races it.
    ATTEMPTS = 3

    HEADER = <<~SQL
      --
      -- Trellis definitions (Trellis::Instance#dump_definitions_sql)
      --

    SQL

    class << self
      # Installs the hooks. Idempotent.
      def install
        ::ActiveRecord::Tasks::DatabaseTasks.singleton_class.prepend(DatabaseTasksHook)
        ::ActiveRecord::Tasks::PostgreSQLDatabaseTasks.prepend(StructureDumpHook)
      end

      # Whether Trellis's definitions belong in the dump of db_config: Trellis
      # is configured, and db_config is the database config.trellis.dump_database
      # names.
      def applies?(db_config)
        return false unless Railtie.connect_options || !Railtie.instance_options.empty?

        db_config.name == dump_database
      end

      # Raises Trellis::ValidationError unless config.trellis.connect and every
      # config.trellis.instances entry point at the database db_config
      # describes: appending the definitions of one database to the dump of
      # another produces a file that fails to load.
      #
      # Databases are compared by host, port and dbname, each as libpq
      # resolves it (an unset one takes its PGHOST/PGPORT/PGDATABASE or
      # default). Spellings are not equated: `localhost` and `127.0.0.1`
      # differ, so give config.trellis.connect and database.yml the same host.
      def verify_same_database!(db_config)
        target = database_of_rails(db_config.configuration_hash)
        configured = {}
        configured["config.trellis.connect"] = Railtie.connect_options if Railtie.connect_options
        Railtie.instance_options.each { |name, options| configured["config.trellis.instances[#{name.inspect}]"] = options }
        configured.each do |what, options|
          next unless options[:url]

          found = database_of_url(options[:url])
          next if found == target

          raise Trellis::ValidationError,
                "#{what} points at #{describe(found)}, but config.trellis.dump_database " \
                "#{db_config.name.inspect} is #{describe(target)}: its definitions don't belong in " \
                "that database's structure dump. Point #{what} at the same host, port and database " \
                "(spelled identically, e.g. `localhost` is not `127.0.0.1`), or set " \
                "config.trellis.dump_database to the config that names it"
        end
      end

      def dump_database
        app = ::Rails.application
        (app&.config&.trellis&.dump_database || "primary").to_s
      end

      # The `--schema` flags to add to the pg_dump of db_config, whose
      # connection settings are configuration_hash.
      def schema_flags(db_config, configuration_hash)
        return [] unless applies?(db_config) && restricts_schemas?(configuration_hash)

        schemas = []
        default = Railtie.connect_options
        schemas << default if default
        schemas.concat(Railtie.instance_options.values)
        schemas.flat_map { |options| [options.fetch(:schema, "trellis"), options.fetch(:target_schema, "public")] }
               .uniq.map { |schema| "--schema=#{schema}" }
      end

      # The definitions of every configured instance as SQL: each one's
      # dump_definitions_sql.
      def appendix
        parts = []
        parts << Railtie.with_handle(&:dump_definitions_sql) if Railtie.connect_options
        Railtie.instance_options.each_key { |name| parts << Railtie.with_handle(name, &:dump_definitions_sql) }
        parts.join("\n")
      end

      # Appends appendix to the dump at filename, and makes its
      # `CREATE SCHEMA public` loadable into a database that has one.
      def finish(filename, appendix)
        sql = File.read(filename, encoding: "utf-8")
        sql = sql.gsub(/^CREATE SCHEMA public;$/, "CREATE SCHEMA IF NOT EXISTS public;")
        File.write(filename, "#{sql}#{HEADER}#{appendix}\n", encoding: "utf-8")
      end

      private

      # [host, port, dbname] of a libpq connection string or URI.
      def database_of_url(url)
        resolved(PG::Connection.conninfo_parse(url.to_s).to_h { |entry| [entry[:keyword], entry[:val]] })
      rescue PG::Error => e
        raise Trellis::ValidationError, "can't read the database from #{url.inspect}: #{e.message.strip}"
      end

      # [host, port, dbname] of a Rails database config hash, which carries
      # either keys or a `url` (or both: the keys win, as in Rails).
      def database_of_rails(hash)
        hash = hash.transform_keys(&:to_sym)
        host, port, dbname = hash[:url] ? database_of_url(hash[:url]) : [nil, nil, nil]
        resolved("host" => hash[:host]&.to_s || host, "port" => hash[:port]&.to_s || port,
                 "dbname" => hash[:database]&.to_s || dbname)
      end

      # Fills what a connection string leaves out the way libpq does.
      def resolved(keywords)
        defaults = PG::Connection.conndefaults_hash
        %i[host port dbname].map do |key|
          value = keywords[key.to_s]
          value = defaults[key] if value.nil? || value.empty?
          value.to_s
        end
      end

      def describe(database)
        host, port, dbname = database
        "#{dbname} on #{host.empty? ? "the local socket" : host}:#{port}"
      end

      # Whether the structure dump passes any `--schema` (so a schema not
      # listed is left out), as PostgreSQLDatabaseTasks#structure_dump decides.
      def restricts_schemas?(configuration_hash)
        case ::ActiveRecord.dump_schemas
        when :schema_search_path then !configuration_hash[:schema_search_path].to_s.strip.empty?
        when :all then false
        else !::ActiveRecord.dump_schemas.to_s.strip.empty?
        end
      end
    end

    # Prepended to ActiveRecord::Tasks::DatabaseTasks's singleton.
    module DatabaseTasksHook
      # Rails 8.1 defaults format to db_config.schema_format, earlier
      # versions to ActiveRecord.schema_format: the default is left to Rails'
      # own signature, which an unconfigured database goes straight to.
      def dump_schema(db_config, *args)
        return super unless SchemaDump.applies?(db_config) && db_config.schema_dump

        format = args.first ||
                 (db_config.respond_to?(:schema_format) ? db_config.schema_format : ::ActiveRecord.schema_format)

        unless format.to_sym == :sql
          raise Trellis::ValidationError,
                "#{db_config.name}'s schema dump is #{format}, which can't carry Trellis's " \
                "tables, triggers or definitions: set config.active_record.schema_format = :sql"
        end

        filename = schema_dump_path(db_config, format)
        return super unless filename

        SchemaDump.verify_same_database!(db_config)

        SchemaDump::ATTEMPTS.times do
          before = SchemaDump.appendix
          super(db_config, format)
          if SchemaDump.appendix == before
            SchemaDump.finish(filename, before)
            return nil
          end
        end
        raise Trellis::ConflictError,
              "Trellis's definitions changed during each of #{SchemaDump::ATTEMPTS} dumps of " \
              "#{db_config.name}: run the dump when no define or drop is running"
      end
    end

    # Prepended to ActiveRecord::Tasks::PostgreSQLDatabaseTasks.
    module StructureDumpHook
      def structure_dump(filename, extra_flags)
        flags = SchemaDump.schema_flags(db_config, configuration_hash)
        super(filename, flags.empty? ? extra_flags : Array(extra_flags) + flags)
      end
    end
  end
end

Trellis::SchemaDump.install
