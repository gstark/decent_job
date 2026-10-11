# frozen_string_literal: true

module DecentJob
  # Lends the Active Record connection to enqueue work, so a job insert
  # commits or rolls back with the application's transaction.
  #
  # {DecentJob.database} passes this module as the enqueue_connection of the
  # global {Database} when {Configuration#connection} is :active_record. Then
  # {Database#with_enqueue_connection} calls {.with}, which borrows a
  # connection from ActiveRecord::Base.connection_pool and calls
  # {Database#lend}. Active Record leases connections per Active Support
  # isolation context, the same context {Database} uses, so the enqueue sees
  # the connection, and the transaction, that the application code holds.
  #
  # Constraints:
  #
  # - Only enqueue work borrows this connection. Workers, LISTEN, heartbeats,
  #   session advisory locks, cleanup, and the cron manager use the pool of
  #   the global {Database}, which {.connection_options} points at the same
  #   database.
  # - Callbacks that must wait for commit go to
  #   ActiveRecord::Base.current_transaction (Rails 7.2+). Database#after_commit
  #   cannot see transactions that Active Record opened.
  # - Requiring this file must not load Active Record. Standalone use and
  #   :own mode never require it.
  #
  # See docs/rails-integration.md.
  module ActiveRecordConnection
    # Registers callbacks with the open Active Record transaction.
    #
    # Active Record moves callbacks of a committed savepoint to its parent and
    # runs rollback callbacks when any enclosing transaction rolls back. This
    # matches {Database#transaction}.
    DEFER = lambda do |commit, rollback|
      transaction = ::ActiveRecord::Base.current_transaction
      next false unless transaction.open?

      transaction.after_commit { commit.each(&:call) } unless commit.empty?
      transaction.after_rollback { rollback.each(&:call) } unless rollback.empty?
      true
    end

    # Connection parameters that identify a database.
    IDENTITY = %i[host port dbname].freeze

    class << self
      # Runs enqueue work on this context's Active Record connection.
      #
      # @param database [Database] the pool that lends the connection
      # @yield the enqueue work
      # @return [Object] the block result
      # @note raw_connection materializes a lazy Active Record transaction,
      #   so the enqueue runs inside it.
      # @note Active Record installs PG type maps that decode results, for
      #   example timestamps to Time. DecentJob reads text results, so the
      #   block runs with text type maps, and Active Record's maps come back
      #   afterward.
      # @api private
      def with(database)
        ::ActiveRecord::Base.connection_pool.with_connection do |connection|
          raw = connection.raw_connection
          results = raw.type_map_for_results
          queries = raw.type_map_for_queries

          begin
            raw.type_map_for_results = PG::TypeMapAllStrings.new
            raw.type_map_for_queries = PG::TypeMapAllStrings.new
            database.lend(raw, defer: DEFER) { yield }
          ensure
            raw.type_map_for_results = results
            raw.type_map_for_queries = queries
          end
        end
      end

      # Hands callbacks to the open Active Record transaction of this context.
      #
      # {Database#after_commit} and {Database#after_rollback} call this when
      # no DecentJob transaction is open.
      #
      # @param commit [Array<#call>] callbacks for the outermost commit
      # @param rollback [Array<#call>] callbacks for a rollback
      # @return [Boolean] false when no Active Record transaction is open;
      #   the caller then runs commit callbacks at once
      # @api private
      def defer(commit, rollback)
        DEFER.call(commit, rollback)
      end

      # Builds PG.connect parameters from the Active Record configuration.
      #
      # The global {Database} uses them in :active_record mode, so workers
      # read the database that enqueues write to.
      #
      # @return [Hash{Symbol => String, Integer}] libpq keywords. A
      #   schema_search_path becomes a search_path option.
      # @api private
      def connection_options
        config = ::ActiveRecord::Base.connection_db_config.configuration_hash
        options = config.transform_keys(&:to_sym).compact
        options[:user] = options.delete(:username) if options.key?(:username)
        options[:dbname] = options.delete(:database) if options.key?(:database)
        options = options.slice(*PG::Connection.conndefaults_hash.keys)

        if (path = config[:schema_search_path])
          options[:options] = [options[:options], "-c search_path=#{path.to_s.delete(" ")}"].compact.join(" ")
        end

        options
      end

      # Tells whether url names the Active Record database.
      #
      # Compares host, port, and database name after libpq defaults apply.
      # A Unix socket and "localhost" count as different hosts.
      #
      # @param url [String, Hash] a connection string, URL, or keyword Hash
      # @return [Boolean]
      # @api private
      def same_database?(url)
        identity(url) == identity(connection_options)
      end

      private

      def identity(value)
        options = if value.is_a?(Hash)
          value.transform_keys(&:to_sym)
        else
          PG::Connection.conninfo_parse(value).to_h { |option| [option[:keyword].to_sym, option[:val]] }
        end

        defaults = PG::Connection.conndefaults_hash
        resolved = IDENTITY.to_h { |key| [key, (options[key] || defaults[key])&.to_s] }
        resolved[:dbname] ||= (options[:user] || defaults[:user])&.to_s
        resolved
      end
    end
  end
end
