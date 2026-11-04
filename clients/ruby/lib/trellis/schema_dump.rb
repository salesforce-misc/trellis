# frozen_string_literal: true

require "active_record"
require "etc"
require "fileutils"
require "trellis/pg"

module Trellis
  # Makes `structure.sql` carry Trellis's definitions, the way Rails makes it
  # carry `schema_migrations`: after `pg_dump` writes the schema, the dump
  # appends the rows that describe how the schema came to be. The dump has
  # the application's schema and the target tables, and one row per
  # definition (Trellis::Instance#schema_dump). Trellis's own schema, the
  # capture triggers on source tables and the 1-1 targets' ledgers are left
  # out: a database built from the file gets them from the loading release,
  # whose `migrate` restores each definition into its dumped target (ADR-0025).
  #
  # Needs `config.active_record.schema_format = :sql`: a `schema.rb` can't
  # carry the definitions. With `:ruby`, the dump raises
  # Trellis::ValidationError and writes nothing: a `schema.rb` would load
  # into a database with target tables and no definitions.
  #
  # It hooks the places Rails offers no extension point for:
  #
  # - ActiveRecord::Tasks::DatabaseTasks.dump_schema (the `db:schema:dump`
  #   task and the dump after `db:migrate` both call it), whose `:sql`
  #   branch is `pg_dump` followed by the `schema_migrations` rows. This
  #   hook takes the capture triggers out of the `pg_dump` output
  #   (Trellis::Instance#filter_schema_dump) and adds the definitions'
  #   rows after the `schema_migrations` ones, for the default instance and
  #   each of config.trellis.instances. Those rows are read before and after
  #   the `pg_dump`, which reads its own snapshot, and the dump is redone (up
  #   to three times) when a define or drop committed in between, so the DDL
  #   and the rows agree.
  # - ActiveRecord::Tasks::PostgreSQLDatabaseTasks#structure_dump. This hook
  #   adds each instance's `pg_dump_args` (which exclude the Trellis schema
  #   and the ledgers) and, when Rails passes `--schema` for the search
  #   path's schemas, each instance's `target_schema`.
  # - ActiveRecord::Tasks::DatabaseTasks.load_schema, which
  #   `db:schema:load`, `db:prepare` and the test database's
  #   `maintain_test_schema!` load the file with. After the load, this hook
  #   runs `migrate` for each configured instance, which restores the
  #   definitions (a file in the older format, which carries Trellis's
  #   schema, needs nothing more and loads as before). A load of another
  #   environment's database (`db:prepare` in development prepares the test
  #   database too, and `db:test:prepare` the test one) runs it against
  #   that database: the instances' options with the database's host, port,
  #   name, user and password from its config.
  #
  # A load that doesn't go through Rails (`psql -f structure.sql`) is followed
  # by `rails trellis:migrate`.
  #
  # All three apply to one database: the config named by
  # `config.trellis.dump_database` (default "primary"), the one
  # config.trellis.connect and config.trellis.instances point at. The other
  # databases of a multi-database app dump and load as Rails does it.
  #
  # A dump lists definitions and nothing else of Trellis's state. Every
  # definition loads unbuilt, as a `define` leaves it, including one that
  # was paused or quarantined where the dump was taken.
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

    # The thread-local the dump hook leaves the parts it read before the
    # `pg_dump` in, for the structure-dump hook's arguments.
    PARTS_KEY = :trellis_schema_dump_parts

    class << self
      # Installs the hooks. Idempotent.
      def install
        ::ActiveRecord::Tasks::DatabaseTasks.singleton_class.prepend(DatabaseTasksHook)
        ::ActiveRecord::Tasks::PostgreSQLDatabaseTasks.prepend(StructureDumpHook)
      end

      # The schema format of a dump or load of db_config: the format
      # argument args carries, or Rails' default for db_config.
      def format_of(db_config, args)
        args.first || (db_config.respond_to?(:schema_format) ? db_config.schema_format : ::ActiveRecord.schema_format)
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

      # The flags to add to the pg_dump of db_config, whose connection
      # settings are configuration_hash: each instance's pg_dump_args, and its
      # target schema when Rails names the schemas to dump. parts are the
      # instances' schema_dump Hashes (Trellis::Instance#schema_dump), read now when the dump hook
      # hasn't.
      def schema_flags(db_config, configuration_hash, parts = nil)
        return [] unless applies?(db_config)

        parts ||= Thread.current[PARTS_KEY] || self.parts
        flags = []
        if restricts_schemas?(configuration_hash)
          flags.concat(configured_options.map { |options| "--schema=#{exact_pattern(options.fetch(:target_schema, "public"))}" })
        end
        flags.concat(parts.flat_map { |part| part.fetch(:pg_dump_args) })
        flags.uniq
      end

      # schema as a pg_dump pattern that matches exactly it: as is when it is
      # lower case letters, digits and underscores, which pg_dump reads
      # literally, and double-quoted otherwise, so its case and pattern
      # characters (* ? .) are taken literally.
      def exact_pattern(schema)
        schema = schema.to_s
        schema.match?(/\A[a-z_][a-z0-9_]*\z/) ? schema : "\"#{schema.gsub('"', '""')}\""
      end

      # Each configured instance's schema_dump Hashes (Trellis::Instance#schema_dump): the default
      # instance's first, then config.trellis.instances' in order.
      def parts
        each_instance.map { |name| Railtie.with_handle(name, &:schema_dump) }
      end

      # The dump at filename without each instance's capture triggers, and
      # its `CREATE SCHEMA public` loadable into a database that has one,
      # followed by each of parts' appendix.
      def finish(filename, parts)
        sql = File.read(filename, encoding: "utf-8")
        sql = sql.gsub(/^CREATE SCHEMA public;$/, "CREATE SCHEMA IF NOT EXISTS public;")
        each_instance.each do |name|
          sql = Railtie.with_handle(name) { |instance| instance.filter_schema_dump(sql) }
        end
        appendix = parts.map { |part| part.fetch(:appendix) }.join("\n")
        File.write(filename, "#{sql}\n#{appendix}", encoding: "utf-8")
      end

      # Restores the definitions a load of db_config's dump queued: `migrate`
      # on each configured instance, in the database db_config names.
      def restore(db_config)
        verify_same_database!(db_config)
        if db_config.env_name.to_s == ::ActiveRecord::Tasks::DatabaseTasks.env.to_s
          Railtie.migrate_all
          return
        end

        configured = []
        configured << [nil, Railtie.connect_options] if Railtie.connect_options
        Railtie.instance_options.each { |name, options| configured << [name, options] }
        configured.each do |name, options|
          what = name ? "config.trellis.instances[#{name.inspect}]" : "config.trellis.connect"
          url = environment_url(db_config, options[:url], what)
          instance = Trellis::Instance.connect(**options, url:, staging: false, drain_threads: 0)
          begin
            instance.migrate
          ensure
            instance.shutdown
          end
        end
      end

      private

      # nil for the default instance (when config.trellis.connect is set),
      # then the name of each of config.trellis.instances.
      def each_instance
        names = []
        names << nil if Railtie.connect_options
        names.concat(Railtie.instance_options.keys)
      end

      def configured_options
        options = []
        options << Railtie.connect_options if Railtie.connect_options
        options.concat(Railtie.instance_options.values)
      end

      # A libpq connection string for the database db_config names, in
      # another environment than configured_url's: configured_url's
      # settings with db_config's database, and its host, port, user,
      # password and other libpq keywords (sslmode, sslrootcert, ...) where
      # db_config names them (a host left to libpq's default keeps
      # configured_url's, a local one by verify_same_database!). A db_config
      # with a url is that url.
      def environment_url(db_config, configured_url, what)
        hash = db_config.configuration_hash.transform_keys(&:to_sym)
        return hash[:url].to_s if hash[:url]

        keywords = conninfo(configured_url, what).reject { |_, value| value.to_s.empty? }
        host, port, dbname = rails_database(hash)
        keywords["dbname"] = dbname
        keywords["port"] = port
        if host
          keywords["host"] = host
          # A hostaddr would win over the host for the connection itself.
          keywords.delete("hostaddr")
        end
        user = hash[:username] || hash[:user]
        keywords["user"] = user.to_s if user
        # Every other libpq keyword the config names (sslmode, sslrootcert,
        # connect_timeout, ...) is the connection Rails makes: the adapter
        # hands them all to libpq. A blank one names nothing, a blank password
        # included: it doesn't blank the url's.
        PG::Connection.conndefaults_hash.each_key do |key|
          next if %i[host port dbname user].include?(key)

          value = hash[key]
          keywords[key.to_s] = value.to_s unless value.nil? || value.to_s.empty?
        end
        keywords.map { |key, value| "#{key}='#{value.to_s.gsub(/[\\']/) { |c| "\\#{c}" }}'" }.join(" ")
      end

      # The keywords of a libpq connection string or URI. A failure names
      # what was being read and never the string or libpq's message, which
      # can quote part of a password (as Trellis's own parse does).
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
                  dbname: hash[:database] || hash[:dbname] || url["dbname"],
                  user: hash[:username] || hash[:user] || url["user"] }
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

        format = SchemaDump.format_of(db_config, args)
        unless format.to_sym == :sql
          raise Trellis::ValidationError,
                "#{db_config.name}'s schema dump is #{format}, which can't carry Trellis's " \
                "definitions: set config.active_record.schema_format = :sql"
        end

        filename = schema_dump_path(db_config, format)
        return super unless filename

        SchemaDump.verify_same_database!(db_config)

        written = false
        finished = false
        begin
          SchemaDump::ATTEMPTS.times do
            before = SchemaDump.parts
            begin
              Thread.current[SchemaDump::PARTS_KEY] = before
              written = true
              super(db_config, format)
            ensure
              Thread.current[SchemaDump::PARTS_KEY] = nil
            end
            if SchemaDump.parts == before
              SchemaDump.finish(filename, before)
              finished = true
              return nil
            end
          end
          raise Trellis::ConflictError,
                "Trellis's definitions changed during each of #{SchemaDump::ATTEMPTS} dumps of " \
                "#{db_config.name}: run the dump when no define or drop is running"
        ensure
          # Whatever Rails wrote is raw `pg_dump` output with the capture
          # triggers in and no definitions: left in place, a later
          # `db:schema:load` would load it without a word. A failure before
          # the first `pg_dump` leaves the previous file alone.
          FileUtils.rm_f(filename) if written && !finished
        end
      end

      # After Rails loads the dump database's `structure.sql`, restores the
      # definitions it queued (SchemaDump.restore).
      def load_schema(db_config, *args)
        result = super
        if SchemaDump.applies?(db_config) && db_config.schema_dump &&
           SchemaDump.format_of(db_config, args).to_sym == :sql
          SchemaDump.restore(db_config)
        end
        result
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
