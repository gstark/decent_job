# frozen_string_literal: true

require "decent_job/cron_schedule"

module DecentJob
  # Defines one recurring job: its key, schedule, job class, and arguments.
  # It computes occurrences, stores enable and disable overrides, and
  # enqueues one run for an intended occurrence time.
  #
  # {Configuration#cron_entries} builds entries from +config.cron+, and
  # {CronManager} calls {#next_at}, {#each_within}, {#enabled?}, and
  # {#enqueue} from its own threads. The dashboard and applications also
  # create entries directly. An entry holds only its params. It keeps no
  # timers, threads, or connections. Every SQL call checks out a connection
  # from {#database} through a new {CronStore}.
  #
  # Params can be static values or callables. {#valid?} and
  # {#display_properties} never call them. {#enqueue} calls +class+, +args+,
  # +kwargs+, and +set+ once per enqueue. A callable +cron+ receives the
  # previous occurrence time and returns a +Time+ or a schedule expression.
  #
  # Cross-process deduplication: {#enqueue} sets the cron context, so
  # {Store} writes +cron_key+ and +cron_at+ on each job row. The partial
  # unique index +decent_jobs_cron_occurrence_idx+ on (+cron_key+, +cron_at+)
  # and +ON CONFLICT DO NOTHING+ accept only the first row for a key and
  # intended occurrence. Many processes can run the same schedules. A manual
  # run has a null +cron_at+, so the index does not apply to it.
  #
  # See docs/recurring-jobs.md and docs/cron-compatibility.md.
  class CronEntry
    # Allowed static types for the optional value params. A callable is also
    # allowed for each one.
    VALUE_TYPES = {args: Array, kwargs: Hash, set: Hash}.freeze

    # +params+ is the definition, merged from the positional hash and the
    # keyword attributes. +errors+ maps each field to its messages from the
    # last {#valid?} call. It is empty before the first call.
    attr_reader :params, :errors

    # Builds the configured entries.
    #
    # @param configuration [Configuration, nil] the configuration to read, or
    #   nil for {DecentJob.configuration}
    # @return [Array<CronEntry>] new entry instances on each call
    # @raise [ArgumentError] when +config.cron+ is not a Hash or an entry is invalid
    def self.all(configuration: nil)
      (configuration || DecentJob.configuration).cron_entries
    end

    # Finds a configured entry by key.
    #
    # @param key [Symbol, String] the entry key; strings compare by name
    # @param configuration [Configuration, nil] the configuration to read, or
    #   nil for {DecentJob.configuration}
    # @return [CronEntry] a new entry instance
    # @raise [KeyError] when no configured entry has this key
    def self.find(key, configuration: nil)
      all(configuration: configuration).find { |entry| entry.key.to_s == key.to_s } || raise(KeyError, "Cron entry #{key} was not found")
    end

    # Returns the cron context of the current thread or fiber. {#enqueue}
    # sets it for the duration of one enqueue. Job execution, retry, and
    # inline execution clear it, so jobs that a cron job creates do not
    # inherit cron metadata.
    #
    # @return [Hash{Symbol => Object}, nil] +:key+, +:at+ (the intended
    #   occurrence, nil for a manual run), and +:database+; nil outside an enqueue
    # @api private
    def self.current_context
      ExecutionState[:cron_context]
    end

    # Sets the cron context for the block and restores the previous value after it.
    #
    # @param context [Hash{Symbol => Object}, nil] the context, or nil to clear it
    # @yield runs with the context set
    # @return [Object] the block result
    # @api private
    def self.with_context(context)
      ExecutionState.with(:cron_context, context) { yield }
    end

    # Returns the cron context only when it belongs to +database+. {Store}
    # and {Adapter} use it so that a job enqueued to another database gets no
    # cron columns and no occurrence deduplication.
    #
    # @param database [Database] the database that persists the job
    # @return [Hash{Symbol => Object}, nil] the context, or nil when there is
    #   none or it belongs to another database
    # @api private
    def self.context_for(database)
      context = current_context
      context if context && context.fetch(:database).equal?(database)
    end

    # Checks that each job uses a DecentJob {Adapter} bound to the database of
    # the current cron context. A cron job written to another database would
    # escape the occurrence index and the cron transaction.
    #
    # @param jobs [Array<Object>] jobs captured during a cron enqueue
    # @return [Array<Object>] +jobs+
    # @raise [ArgumentError] when a job is not a job object or uses another
    #   adapter or database
    # @note Call this only inside a cron context. Every caller checks
    #   {.current_context} first; without a context this raises NoMethodError.
    # @api private
    def self.validate_jobs(jobs)
      database = current_context.fetch(:database)

      jobs.each do |job|
        adapter = job.queue_adapter if Integrations.job?(job)

        unless adapter.is_a?(Adapter) && adapter.database.equal?(database)
          raise ArgumentError, "Cron jobs must use a DecentJob adapter with the cron entry database"
        end
      end
    end

    # Returns the latest stored job of each entry, with one query per database.
    #
    # @param entries [Array<CronEntry>] entries, grouped by {#database}
    # @param before [Time, nil] inclusive upper bound for +cron_at+, or nil for
    #   no bound; see {CronStore#last_jobs_by_key}
    # @return [Hash{String => Hash{String => Object}}] decoded rows keyed by
    #   string cron key; an entry with no history is absent
    # @api private
    def self.last_jobs_by_key(entries, before: nil)
      entries.group_by(&:database).each_with_object({}) do |(database, group), result|
        result.merge!(CronStore.new(database).last_jobs_by_key(group.map(&:key), before: before))
      end
    end

    # @param params [Hash{Symbol => Object}] the definition: +:key+, +:cron+,
    #   +:class+, +:args+, +:kwargs+, +:set+, +:description+, and
    #   +:enabled_by_default+
    # @param database [Database, nil] the database for history, overrides, and
    #   enqueue; nil resolves it on each call, see {#database}
    # @param attributes [Hash{Symbol => Object}] more definition keys; they
    #   override keys in +params+
    # @note The constructor does not validate. Call {#valid?}.
    def initialize(params = {}, database: nil, **attributes)
      @params = params.merge(attributes)
      @database = database
      @errors = {}
    end

    # Readers for the raw params. Values can be callables; the readers do not
    # call them.
    %i[key args kwargs set description cron].each do |name|
      define_method(name) { params[name] }
    end

    alias_method :id, :key
    alias_method :to_param, :key

    # @return [Class, String, #call, nil] the raw +:class+ param
    def job_class
      params[:class]
    end

    # Validates the definition and replaces {#errors}.
    #
    # Callables are accepted without a call. A class name string must
    # resolve to a loaded constant, so this check can trigger autoloading.
    #
    # @return [Boolean] true when {#errors} is empty
    def valid?
      @errors = {}
      add_error(:key, "must be a Symbol") unless key.is_a?(Symbol)
      add_error(:cron, "is not a valid schedule") unless cron.respond_to?(:call) || schedule

      unless Support.blank?(job_class) || job_class.is_a?(Class) || job_class.respond_to?(:call)
        begin
          resolved = job_class.is_a?(String) && Support.constantize(job_class)
          add_error(:class, "must resolve to a Class") unless resolved.is_a?(Class)
        rescue NameError
          add_error(:class, "does not exist")
        end
      end

      VALUE_TYPES.each do |name, type|
        value = public_send(name)
        add_error(name, "must be a #{type}") unless value.nil? || value.respond_to?(:call) || value.is_a?(type)
      end

      errors.empty?
    end

    # Returns the definition in a form that is safe to show. Callables show
    # as "Lambda/Callable" and nil shows as "None". No callable is called.
    #
    # @return [Hash{Symbol => Object}] display values; +:args+ and +:kwargs+
    #   are present only when set
    def display_properties
      {key: key, class: display_property(job_class), cron: display_schedule,
       set: display_property(set), description: display_property(description)}.tap do |properties|
        properties[:args] = display_property(args) if Support.present?(args)
        properties[:kwargs] = display_property(kwargs) if Support.present?(kwargs)
      end
    end

    # @return [String, Object] the schedule text: the generated numeric cron for
    #   a natural expression, the original text for a numeric one, or the
    #   display value of a callable or invalid +cron+
    def display_schedule
      schedule ? schedule.original : display_property(cron)
    end

    # Computes the next occurrence.
    #
    # An expression schedule searches strictly after +after+, or after the
    # current wall clock when +after+ is nil. When the wall clock moved
    # backward and the default search lands at or before +previously_at+, the
    # search restarts after +previously_at+. A forward clock change still
    # skips the occurrences in between.
    #
    # A callable +cron+ receives +previously_at+, or {#last_job_at} when
    # +previously_at+ is nil. A +Time+ result is returned as it is: +after+
    # does not apply, and this method does not check that it advances.
    # {CronManager} does that check.
    #
    # @param previously_at [Time, nil] the previous intended occurrence, or nil
    # @param after [Time, nil] an explicit search origin, or nil for the
    #   current wall clock
    # @return [Time, nil] the next occurrence as a UTC instant (or the
    #   callable's +Time+), or nil when the expression is invalid
    # @raise [ArgumentError] when a valid expression has no reachable occurrence
    # @note With a callable +cron+ and no +previously_at+, this runs a SQL query.
    def next_at(previously_at: nil, after: nil)
      value = cron.respond_to?(:call) ? cron.call(previously_at || last_job_at) : cron
      return value if cron.respond_to?(:call) && value.is_a?(Time)

      parsed = CronSchedule.parse(value)
      occurrence = parsed&.next_at(after: after)

      # A clock change inside a callable expression must not move its chain
      # behind the preceding intended occurrence. Keep the default zone-aware
      # calculation for ordinary scheduling and forward clock changes.
      if !after && previously_at && occurrence && occurrence <= previously_at
        occurrence = parsed.next_at(after: previously_at)
      end

      occurrence
    end

    # Returns the occurrences strictly inside +period+. Both ends are
    # excluded, also for an inclusive range.
    #
    # @param period [Range<Time>] the time range
    # @param previously_at [Time, nil] passed to a callable +cron+; nil uses
    #   {#last_job_at} for a callable
    # @return [Array<Time>] UTC instants in ascending order; empty when the
    #   callable returns a +Time+ or the expression is invalid
    # @raise [ArgumentError] when a valid expression has no reachable occurrence
    def within(period, previously_at: nil)
      each_within(period, previously_at: previously_at || (last_job_at if cron.respond_to?(:call))).to_a
    end

    # Yields the occurrences strictly inside +period+ without building an
    # array. A callable +cron+ is called once, so the whole range uses one
    # expression. {CronManager} uses this for restart catch-up.
    #
    # @param period [Range<Time>] the time range; both ends are excluded
    # @param previously_at [Time, nil] passed to a callable +cron+; the
    #   default is {#last_job_at} for a callable. An explicit nil passes nil.
    # @yieldparam occurrence [Time] a UTC instant, in ascending order
    # @return [Enumerator, nil] an enumerator without a block, otherwise nil
    # @raise [ArgumentError] when a valid expression has no reachable occurrence
    def each_within(period, previously_at: (last_job_at if cron.respond_to?(:call)))
      return enum_for(__method__, period, previously_at: previously_at) unless block_given?

      value = cron.respond_to?(:call) ? cron.call(previously_at) : cron
      parsed = CronSchedule.parse(value)
      return unless parsed

      cursor = period.begin

      loop do
        cursor = parsed.next_at(after: cursor)
        break if cursor >= period.end

        yield cursor
      end
    end

    # @return [Boolean] the +:enabled_by_default+ param, called when it is a
    #   callable; true when absent
    def enabled_by_default?
      value = params.fetch(:enabled_by_default, true)
      value.respond_to?(:call) ? value.call : value
    end

    # Resolves the database of this entry on each call. The order is: the
    # +database:+ argument, the current job execution, the current cron
    # context, the current batch context, then {DecentJob.database}. An
    # unbound entry can so resolve to different databases in different
    # contexts.
    #
    # @return [Database] the connection pool
    def database
      @database || Current.execution&.fetch(:database, nil) || self.class.current_context&.fetch(:database, nil) || Batch.current_context&.fetch(:database, nil) || DecentJob.database
    end

    # Reads the persistent override, or {#enabled_by_default?} when there is
    # none. {CronManager} checks it before each delivery. {#enqueue} does not
    # check it.
    #
    # @return [Boolean] true when the scheduler can enqueue occurrences
    # @note Runs one SQL query.
    def enabled?
      store.enabled?(key, default: enabled_by_default?)
    end

    # Stores an enabled override for this key. It applies to all processes.
    # It does not replay occurrences that were skipped while disabled.
    #
    # @return [Boolean] true
    # @note Writes +decent_job_cron_settings+.
    def enable
      store.set_enabled(key, true)
    end

    # Stores a disabled override for this key. It applies to all processes.
    # The scheduler keeps advancing the schedule but does not enqueue.
    #
    # @return [Boolean] false
    # @note Writes +decent_job_cron_settings+.
    def disable
      store.set_enabled(key, false)
    end

    # @return [Array<Hash{String => Object}>] all stored jobs of this entry,
    #   newest occurrence first, manual runs last; see {CronStore#jobs}
    def jobs
      store.jobs(key)
    end

    # @return [Hash{String => Object}, nil] the latest stored job, with
    #   scheduled runs ahead of manual runs; nil when there is no history
    def last_job
      store.last_jobs_by_key([key])[key.to_s]
    end

    # @return [Time, nil] +cron_at+ of {#last_job}, or its +created_at+ for a
    #   manual run, in the process local zone; nil when there is no history
    def last_job_at
      row = last_job
      (row["cron_at"] || row["created_at"]).localtime if row
    end

    # Enqueues one run of this entry.
    #
    # The run happens in a database transaction (a savepoint inside an
    # enclosing transaction) with the cron context set, so each captured job
    # row gets +cron_key+ and +cron_at+. The entry uses its own bulk buffer,
    # so it flushes independently of an enclosing {Bulk} block but shares its
    # transaction. A duplicate occurrence is skipped by the unique index, and
    # the enclosing transaction stays usable.
    #
    # @param cron_at [Time, nil] the intended occurrence, which identifies the
    #   run for deduplication; it is not a delay. Nil is a manual run that is
    #   never deduplicated.
    # @return [Object, false, Array<Object>, nil] for a job class, or a callable
    #   that returns a job: the job when accepted, false when rejected (for
    #   example a duplicate occurrence). A callable that returns false gives
    #   false. Other callables give the accepted jobs as an array (empty when
    #   all were rejected), or nil when they enqueued nothing. A blank +class+
    #   gives nil.
    # @raise [ArgumentError] when +cron_at+ is not a Time or nil, or a job uses
    #   another adapter or database
    # @raise [EnqueueError] when the insert fails with a database error. The
    #   occurrence conflict does not raise. The error can be translated to the
    #   job integration's enqueue error class.
    # @note Runs in the default I18n locale when I18n is loaded. Ignores
    #   {#enabled?}. If the transaction rolls back, new jobs lose their
    #   provider IDs and enqueue flags.
    def enqueue(cron_at = nil)
      raise ArgumentError, "cron_at must be a Time or nil" unless cron_at.nil? || cron_at.is_a?(Time)

      connection_pool = database
      buffer = Bulk::Buffer.new
      result = nil
      attempted = false

      Bulk.with_buffer(nil) do
        connection_pool.transaction do
          self.class.with_context(key: key, at: cron_at, database: connection_pool) do
            with_default_locale do
              buffer.capture { result = enqueue_definition }
              buffer.prepare
              self.class.validate_jobs(buffer.jobs)
              # Take the concurrency scope locks of all captured jobs before the
              # insert. They are transaction locks, held until this transaction ends.
              ConcurrencyControls.new(connection_pool).lock_scopes(buffer.jobs)
              attempted = !buffer.jobs.empty?
              pending = buffer.jobs.reject(&:provider_job_id)

              connection_pool.after_rollback do
                pending.each do |job|
                  job.provider_job_id = nil
                  job.successfully_enqueued = false
                end
              end

              buffer.enqueue
            end
          end
        end
      end

      if Integrations.job?(result)
        result.successfully_enqueued? ? result : false
      elsif result == false
        false
      else
        attempted ? buffer.jobs : nil
      end
    rescue
      buffer&.discard
      raise
    end

    private

    # Cron enqueues run in the default locale when I18n is loaded.
    def with_default_locale(&block)
      defined?(::I18n) ? ::I18n.with_locale(::I18n.default_locale, &block) : yield
    end

    def store
      CronStore.new(database)
    end

    def enqueue_definition
      klass = resolve_value(job_class)
      return if Support.blank?(klass)

      klass = Support.constantize(klass) if klass.is_a?(String)
      return klass unless klass.is_a?(Class)

      klass.set(resolve_value(set) || {}).perform_later(*(resolve_value(args) || []), **(resolve_value(kwargs) || {}))
    end

    def resolve_value(value)
      value.respond_to?(:call) ? value.call : value
    end

    def add_error(field, message)
      (@errors[field] ||= []) << message
    end

    def schedule
      CronSchedule.parse(cron)
    end

    def display_property(value)
      return "None" if value.nil?

      value.respond_to?(:call) ? "Lambda/Callable" : value
    end
  end
end
