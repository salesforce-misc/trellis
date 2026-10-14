# frozen_string_literal: true

require "active_record"
require "etc"
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

    # The hosts a Rails config with no host (libpq's default, the local
    # socket) matches, besides a socket directory.
    LOCAL_HOSTS = %w[localhost 127.0.0.1 ::1].freeze

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
      # Databases are compared by host, port and dbname, each resolved the way
      # its client resolves it: Rails' config as libpq does (PGHOST, PGPORT,
      # PGDATABASE, then the user name), a Trellis url as Trellis does, from
      # the url alone (port 5432, dbname the user name). A Rails config with no
      # host connects to libpq's default, the local socket, which matches a
      # local Trellis host (`localhost`, `127.0.0.1`, `::1` or a socket
      # directory). Other spellings are not equated: `localhost` and
      # `127.0.0.1` differ, so give config.trellis.connect and database.yml the
      # same host.
      #
      # `db:prepare` in development also migrates and dumps the test
      # environment's databases, while config.trellis.connect is
      # development's: a db_config of another environment is compared as the
      # current environment's config of the same name.
      def verify_same_database!(db_config)
        require "pg"
        target = rails_database(current_environment_config(db_config).configuration_hash)
        configured = {}
        configured["config.trellis.connect"] = Railtie.connect_options if Railtie.connect_options
        Railtie.instance_options.each { |name, options| configured["config.trellis.instances[#{name.inspect}]"] = options }
        configured.each do |what, options|
          next unless options[:url]

          found = trellis_database(options[:url], what)
          next if same_database?(found, target)

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

      # The keywords of a libpq connection string or URI. A failure names
      # what was being read and never the string or libpq's message, which
      # can quote part of a password (as Trellis's own parse does, #608).
      def conninfo(url, what)
        PG::Connection.conninfo_parse(url.to_s).to_h { |entry| [entry[:keyword], entry[:val]] }
      rescue PG::Error
        raise Trellis::ValidationError,
              "#{what}'s url doesn't parse as a libpq connection string or postgres:// URL"
      end

      # [host, port, dbname] Trellis connects to with url. Trellis reads
      # nothing from the environment: an unset port is 5432 and an unset
      # dbname the user's name (the server's default), the user being the
      # OS user when the url names none. The host is nil only for a url
      # naming neither host nor hostaddr, which Trellis can't connect with.
      def trellis_database(url, what)
        keywords = conninfo(url, what).transform_values { |value| value.to_s.empty? ? nil : value }
        user = keywords["user"] || Etc.getpwuid(Process.euid)&.name
        [keywords["host"] || keywords["hostaddr"], keywords["port"] || "5432", keywords["dbname"] || user.to_s]
      end

      # [host, port, dbname] of a Rails database config hash, as libpq
      # resolves it: Rails' keys (or a `url`, which a hand-built HashConfig
      # can carry), then PGHOST/PGPORT/PGDATABASE and libpq's defaults. The
      # host is nil when nothing names one: libpq's default, the local socket.
      def rails_database(hash)
        hash = hash.transform_keys(&:to_sym)
        url = hash[:url] ? conninfo(hash[:url], "config.trellis.dump_database's url") : {}
        given = { host: hash[:host] || url["host"], port: hash[:port] || url["port"],
                  dbname: hash[:database] || url["dbname"], user: hash[:username] || url["user"] }
        defaults = PG::Connection.conndefaults_hash
        resolved = given.to_h do |key, value|
          value = defaults[key] if value.nil? || value.to_s.empty?
          [key, value.nil? || value.to_s.empty? ? nil : value.to_s]
        end
        [resolved[:host], resolved[:port] || "5432", resolved[:dbname] || resolved[:user].to_s]
      end

      def same_database?(trellis, rails)
        host, port, dbname = trellis
        rails_host, rails_port, rails_dbname = rails
        return false unless port == rails_port && dbname == rails_dbname

        host == rails_host || (rails_host.nil? && local_host?(host))
      end

      def local_host?(host)
        LOCAL_HOSTS.include?(host) || host.to_s.start_with?("/", "@")
      end

      # The config named db_config.name in the environment being run, or
      # db_config itself when it is that environment's (or there is none).
      def current_environment_config(db_config)
        env = ::ActiveRecord::Tasks::DatabaseTasks.env.to_s
        return db_config if db_config.env_name.to_s == env

        ::ActiveRecord::Base.configurations.configs_for(env_name: env, name: db_config.name) || db_config
      end

      def describe(database)
        host, port, dbname = database
        "#{dbname} on #{host || "the local socket"}:#{port}"
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
