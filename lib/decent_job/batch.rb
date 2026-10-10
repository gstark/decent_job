# frozen_string_literal: true

require "decent_job/batch_properties"
require "decent_job/bulk"

module DecentJob
  # A group of jobs that fires callback jobs when its members finish.
  #
  # This is the public batch API and an in-memory copy of one
  # decent_job_batches row. {BatchStore} owns all SQL and every state
  # transition. A Batch object is not thread safe; share the ID, not the
  # object.
  #
  # Lifecycle:
  # 1. {#add} inserts members at once. Members carry decent_jobs.batch_id.
  #    {#enqueue} also opens a new generation, then sets enqueued_at.
  #    Success and finish wait for enqueued_at; discard does not.
  # 2. When a member or callback job finishes, the worker inserts a row into
  #    decent_job_batch_updates in the same transaction as the job's terminal
  #    state ({BatchStore#request}). It then calls {BatchStore#reconcile}.
  # 3. Reconcile sets discarded_at and creates the discard event on the first
  #    discarded member or callback of the current generation. When no member
  #    is ready or running, it sets jobs_finished_at and creates the success
  #    event (only without a discard) and the finish event.
  # 4. Each event enqueues the on_success, on_discard, or on_finish class with
  #    arguments (batch, {event: :success}) and so on. Callback jobs carry
  #    decent_jobs.batch_callback_id.
  # 5. When all callback jobs finish, reconcile sets finished_at.
  #
  # Calling {#enqueue} again or {#retry} reopens the batch: the generation
  # goes up and the timestamps clear. Events are unique per generation, so
  # each callback fires at most once per generation.
  #
  # Constraints:
  # - All members and callback classes must use a DecentJob {Adapter} with
  #   the same {Database} object as the batch. Methods raise ArgumentError
  #   otherwise.
  # - A job belongs to at most one member batch.
  # - Batch operations run their own transaction and flush apart from an
  #   enclosing {Bulk} capture.
  #
  # When Active Job is loaded, decent_job/active_job adds
  # GlobalID::Identification, so batches can be job arguments. {.find},
  # {.where}, and {.primary_key} exist for GlobalID's locator.
  #
  # See README.md "Batches" and docs/record-preservation.md.
  class Batch
    # Writable columns of decent_job_batches. Each gets a reader and a writer.
    ATTRIBUTES = %i[description on_finish on_success on_discard callback_queue_name callback_priority].freeze
    # Timestamp columns of decent_job_batches. Each gets a reader. Nil means
    # the transition has not happened in the current generation.
    TIMESTAMPS = %i[created_at updated_at enqueued_at jobs_finished_at finished_at discarded_at].freeze

    # @return [Database] the pool that holds this batch and its jobs
    attr_reader :database

    # @return [Database] the database of the running job, else the database of
    #   the open batch context, else {DecentJob.database}
    def self.default_database
      Current.execution&.fetch(:database, nil) || current_context&.fetch(:database, nil) || DecentJob.database
    end

    # Builds a batch and enqueues it in one call.
    #
    # @param jobs [Array<Object>] job instances to add
    # @param database [Database, nil] the pool, or nil for {.default_database}
    # @param properties [Hash] {ATTRIBUTES} values and custom properties
    # @yield jobs enqueued in the block join the batch
    # @return [Batch] the enqueued batch, reloaded
    # @raise (see #enqueue)
    def self.enqueue(jobs = [], database: nil, **properties, &block)
      new(database: database, **properties).tap { |batch| batch.enqueue(jobs, &block) }
    end

    # @param id [String] the batch UUID
    # @param database [Database, nil] the pool, or nil for {.default_database}
    # @return [Batch]
    # @raise [KeyError] when no batch has this ID
    def self.find(id, database: nil)
      database ||= default_database
      row = BatchStore.new(database).find(id)
      raise KeyError, "Batch #{id} was not found" unless row

      new(database: database, record: row)
    end

    # Loads several batches. GlobalID's locate_many uses this.
    #
    # @param id [String, Array<String>] one or more batch UUIDs
    # @param database [Database, nil] the pool, or nil for {.default_database}
    # @return [Array<Batch>] the batches found, in argument order. Missing IDs
    #   are skipped.
    def self.where(id:, database: nil)
      database ||= default_database

      Array(id).filter_map do |value|
        row = BatchStore.new(database).find(value)
        new(database: database, record: row) if row
      end
    end

    # @return [Symbol] :id, for GlobalID
    def self.primary_key
      :id
    end

    # @return [Hash, nil] the batch context of this thread or fiber, or nil
    #   outside batch work
    # @api private
    def self.current_context
      ExecutionState[:batch_context]
    end

    # Finds the batch columns for a job that is about to be inserted.
    #
    # {Store} calls this for each insert. With a :batch_ids map (from
    # {.enqueue_all}), a job not in the map gets no member batch. This is why
    # jobs that a member's callback enqueues do not join the batch.
    #
    # @param database [Database] the pool of the insert
    # @param job [Object, nil] the job being inserted
    # @return [Hash{Symbol => String, nil}, nil] :batch_id and
    #   :batch_callback_id, or nil when no context is open for this database
    # @api private
    def self.context_for(database, job: nil)
      context = current_context
      return unless context && context[:database].equal?(database)

      batch_id = (context[:batch_ids] && job) ? context[:batch_ids][job.job_id] : context[:batch_id]
      {batch_id: batch_id, batch_callback_id: context[:batch_callback_id]}
    end

    # @return [String, nil] the member batch ID of the open context
    # @api private
    def self.current_batch_id
      current_context&.fetch(:batch_id)
    end

    # @return [String, nil] the callback batch ID of the open context
    # @api private
    def self.current_batch_callback_id
      current_context&.fetch(:batch_callback_id)
    end

    # Opens a transaction and a batch context. Jobs inserted in the block get
    # the given batch columns.
    #
    # Pass no IDs to clear the batch context, for example around a retry.
    #
    # @param batch_id [String, nil] member batch for inserted jobs
    # @param batch_callback_id [String, nil] callback batch for inserted jobs
    # @param batch_ids [Hash{String => String}, nil] job_id to batch ID map
    # @param database [Database, nil] the pool, or nil for {.default_database}
    # @yield the enqueue work
    # @return [Object] the block result
    # @api private
    def self.within_thread(batch_id: nil, batch_callback_id: nil, batch_ids: nil, database: nil)
      database ||= default_database

      database.transaction do
        with_context(batch_id: batch_id, batch_callback_id: batch_callback_id, batch_ids: batch_ids, database: database) { yield }
      end
    end

    # @param context [Hash, nil] the batch context, or nil to clear it
    # @yield runs with the context set
    # @return [Object] the block result
    # @api private
    def self.with_context(context)
      ExecutionState.with(:batch_context, context) { yield }
    end

    # Creates and enqueues several new batches in one transaction.
    #
    # Batch rows go in one INSERT, and plain members in one bulk insert.
    # Controlled jobs still get individual admission checks. Empty batches
    # fire their callbacks too.
    #
    # @param pairs [Array<Array(Batch, Array<Object>)>, Hash{Batch => Array}]
    #   each new batch with its jobs
    # @return [Array<Batch>] the batches, reloaded. An empty input returns [].
    # @raise [ArgumentError] when a batch is persisted, repeated, or on another
    #   database, when a job is in two batches, or when a job adapter does
    #   not match
    # @note On failure before commit, each batch gets its in-memory row back,
    #   so it stays unpersisted. Takes the batch locks in ID order.
    def self.enqueue_all(pairs)
      pairs = Array(pairs).map { |batch, jobs| [batch, Array(jobs).compact] }
      return [] if pairs.empty?

      batches = pairs.map(&:first)
      raise ArgumentError, "All batches must be new Batch instances" unless batches.all? { |batch| batch.is_a?(self) && !batch.persisted? }
      raise ArgumentError, "Each batch must appear only once" unless batches.uniq.size == batches.size

      database = batches.first.database
      raise ArgumentError, "All batches must use the same database" unless batches.all? { |batch| batch.database.equal?(database) }

      pairs.each { |batch, jobs| batch.send(:validate_members, jobs) }
      member_batches(pairs)
      snapshots = batches.map { |batch| batch.instance_variable_get(:@row).dup }
      committed = false

      Bulk.with_buffer(nil) do
        database.transaction do
          database.after_commit { committed = true }
          store = BatchStore.new(database)
          rows = store.save_all(batches.map { |batch| batch.send(:serialized_attributes) })
          batches.zip(rows).each { |batch, row| batch.instance_variable_set(:@row, row) }

          store.locked(batches.map(&:id)) do
            buffers = pairs.map { |batch, jobs| batch.send(:prepare_members, jobs) }
            batch_ids = member_batches(batches.zip(buffers.map(&:jobs))).transform_values(&:id)
            existing_members = buffers.map { |buffer| buffer.jobs.select(&:provider_job_id) }
            store.lock_scopes(buffers.flat_map(&:jobs))
            batches.each { |batch| store.open(batch.id) }
            combined = Bulk::Buffer.new
            buffers.each { |buffer| combined.append(buffer) }
            within_thread(batch_ids: batch_ids, database: database) { combined.enqueue }

            batches.zip(existing_members).each do |batch, existing|
              store.attach(batch.id, existing) unless existing.empty?
              store.mark_enqueued(batch.id)
              batch.send(:request_reconciliation)
            end
          end
        end
      end

      batches.each(&:reload)
    rescue
      unless committed
        Array(batches).zip(snapshots || []).each { |batch, row| batch.instance_variable_set(:@row, row) if row }
      end

      raise
    end

    # Maps each job_id to its batch, and rejects a job in two batches.
    def self.member_batches(pairs)
      pairs.each_with_object({}) do |(batch, jobs), membership|
        jobs.each do |job|
          previous = membership[job.job_id]
          raise ArgumentError, "A job cannot belong to more than one batch" if previous && !previous.equal?(batch)

          membership[job.job_id] = batch
        end
      end
    end
    private_class_method :member_batches

    # @param database [Database, nil] the pool, or nil for {.default_database}
    # @param record [Hash, nil] a decoded decent_job_batches row, or nil for a
    #   new unsaved batch
    # @param properties [Hash] {ATTRIBUTES} values and custom properties.
    #   Unknown keys become custom properties.
    def initialize(database: nil, record: nil, **properties)
      @database = database || self.class.default_database
      @row = record ? record.dup : {"properties" => BatchProperties.dump({})}
      @properties_loaded = false
      assign_properties(properties)
    end

    # A callback class assigned to on_finish, on_success, or on_discard is
    # stored by name.
    ATTRIBUTES.each do |name|
      define_method(name) { @row[name.to_s] }

      define_method("#{name}=") do |value|
        @row[name.to_s] = (name.to_s.start_with?("on_") && value.is_a?(Class)) ? value.name : value
      end
    end

    TIMESTAMPS.each { |name| define_method(name) { @row[name.to_s] } }

    # @return [String, nil] the batch UUID, or nil before the first save
    def id
      @row["id"]
    end

    # @return [Boolean] true when the row exists in decent_job_batches
    def persisted?
      !id.nil?
    end

    # enqueued?, jobs_finished?, finished?, and discarded? read the loaded
    # timestamps. Call {#reload} for fresh values.
    %i[enqueued jobs_finished finished discarded].each do |name|
      define_method("#{name}?") { !public_send("#{name}_at").nil? }
    end

    # @return [Boolean] true when members and callbacks finished with no
    #   discard in the current generation
    def succeeded?
      finished? && !discarded?
    end

    # Custom properties, deserialized on first read.
    #
    # Deserialization runs with this batch's database as the current
    # execution database, so nested batch GlobalIDs load from the same pool.
    #
    # @return [Hash] the mutable properties. Call {#save} to persist changes.
    # @raise [DeserializationError] when a stored value cannot load
    def properties
      unless @properties_loaded
        @properties = Current.with(database: database) { BatchProperties.load(@row.fetch("properties")) }
        @properties_loaded = true
      end

      @properties
    end

    # @param value [Hash] replaces all custom properties
    # @raise [ArgumentError] when value is not a Hash
    def properties=(value)
      raise ArgumentError, "Properties must be a Hash" unless value.is_a?(Hash)

      @properties = value.dup
      @properties_loaded = true
    end

    # Sets {ATTRIBUTES} and merges every other key into {#properties}.
    #
    # @param attributes [Hash] may include :properties to replace them first
    # @return [Batch] self
    def assign_properties(attributes)
      attributes = attributes.dup
      self.properties = attributes.delete(:properties) if attributes.key?(:properties)
      ATTRIBUTES.each { |name| public_send("#{name}=", attributes.delete(name)) if attributes.key?(name) }
      properties.merge!(attributes) unless attributes.empty?
      self
    end

    # Writes the attributes and properties to decent_job_batches.
    #
    # @return [true]
    # @raise [ArgumentError] when a callback class does not use a DecentJob
    #   adapter with this database
    # @raise [SerializationError] when a property cannot serialize
    # @note An update holds the batch advisory lock, so it waits for a running
    #   reconcile of the same batch.
    def save
      attributes = serialized_attributes

      if persisted?
        store.locked([id]) { @row = store.save(id: id, attributes: attributes) }
      else
        @row = store.save(id: nil, attributes: attributes)
      end

      true
    end

    # @return [Batch] self, with the stored row
    # @raise [KeyError] when the batch was deleted
    def reload
      @row = store.find(id) || raise(KeyError, "Batch #{id} was not found")
      @properties_loaded = false
      self
    end

    # The GlobalID string, written without the globalid gem for native jobs.
    #
    # @return [String] "gid://decent-job/DecentJob::Batch/<id>"
    def decent_job_global_id
      "gid://decent-job/#{self.class.name}/#{URI.encode_www_form_component(id)}"
    end

    # The row with properties for display. Properties that cannot deserialize
    # stay in their serialized form instead of raising.
    #
    # @return [Hash{String => Object}]
    def display_attributes
      properties = Current.with(database: database) { BatchProperties.display(@row.fetch("properties")) }
      @row.merge("properties" => properties)
    end

    # Saves the batch, opens a new generation, inserts the jobs, and marks the
    # batch enqueued. Calling it again reopens the batch.
    #
    # @param jobs [Array<Object>] job instances to add. Jobs that already have
    #   a provider ID are attached instead of inserted.
    # @param properties [Hash] {ATTRIBUTES} values and custom properties
    # @yield jobs enqueued in the block join the batch
    # @return [Array<Object>] the accepted jobs
    # @raise [ArgumentError] when a job adapter does not match the batch
    #   database
    # @note All writes share one transaction under the batch advisory lock.
    #   Reconciliation runs after commit, so an empty batch fires its
    #   callbacks at once. On rollback, the in-memory row is restored.
    def enqueue(jobs = [], **properties, &block)
      assign_properties(properties)
      validate_members(Array(jobs).compact)

      accepted = transaction do
        save_and_lock do
          buffer = prepare_members(jobs, &block)
          store.lock_scopes(buffer.jobs)
          enqueue_buffer(buffer)
        end
      end

      reload
      accepted
    end

    # Inserts or attaches jobs without marking the batch enqueued.
    #
    # Members run at once. Success and finish callbacks wait until {#enqueue}.
    #
    # @param jobs [Array<Object>] job instances to add
    # @yield jobs enqueued in the block join the batch
    # @return [Array<Object>] the accepted jobs
    # @raise [ArgumentError] when a job adapter does not match the batch
    #   database
    def add(jobs = [], &block)
      validate_members(Array(jobs).compact)

      accepted = transaction do
        save_and_lock do
          buffer = prepare_members(jobs, &block)
          store.lock_scopes(buffer.jobs)
          flush_members(buffer)
        end
      end

      reload
      accepted
    end

    # @return [Array<Object>] deserialized member jobs still stored, oldest
    #   first. Cleanup and preservation policies can remove members.
    def job_instances
      persisted? ? store.members(id) : []
    end

    # @return [Array<Object>] deserialized callback jobs still stored
    def callback_job_instances
      persisted? ? store.members(id, callbacks: true) : []
    end

    # Reopens the batch and retries its discarded members.
    #
    # Each member retries through its class's retry_job and enqueue callbacks,
    # and keeps its IDs and history. Old callback jobs do not retry. A
    # force-discarded member whose execution still holds its lock is skipped.
    #
    # @return [Batch] self, reloaded
    # @raise [ArgumentError] when a member class does not use a DecentJob
    #   adapter with this database
    # @note Inline members run after commit. Async adapters wake their worker
    #   after commit.
    def retry
      transaction do
        save unless persisted?

        store.locked([id]) do
          store.open(id)
          ids = store.retry_members(id)
          store.mark_enqueued(id)
          request_reconciliation

          # Each adapter runs inline members or wakes its async worker after
          # commit, as it does for a single job retry.
          store.members(id, ids: ids, deserialize_arguments: false).group_by(&:queue_adapter).each do |adapter, jobs|
            adapter.after_enqueue(jobs.map(&:provider_job_id))
          end
        end
      end

      reload
    end

    private

    def serialized_attributes
      ATTRIBUTES.to_h { |name| [name, public_send(name)] }.merge(properties: BatchProperties.dump(properties))
    end

    def store
      @store ||= BatchStore.new(database)
    end

    # Restores the in-memory row when the outer transaction does not commit,
    # so a failed call leaves the object as it was.
    def transaction
      previous = @row.dup
      committed = false

      Bulk.with_buffer(nil) do
        database.transaction do
          database.after_commit { committed = true }
          yield
        end
      end
    rescue
      @row = previous unless committed
      raise
    end

    # A new batch is inserted before the lock, because the lock key needs the
    # ID. An existing batch is updated under the lock.
    def save_and_lock
      existing = persisted?
      save unless existing

      store.locked([id]) do
        save if existing
        yield
      end
    end

    def validate_members(jobs)
      jobs.each do |job|
        unless Integrations.job?(job) && job.queue_adapter.is_a?(Adapter) && job.queue_adapter.database.equal?(database)
          raise ArgumentError, "Batch jobs must use a DecentJob adapter with the batch database"
        end
      end
    end

    # Runs enqueue callbacks of captured jobs before any scope lock is taken.
    # The jobs are not inserted yet.
    def prepare_members(jobs, &block)
      buffer = Bulk::Buffer.new
      buffer.add(Array(jobs).compact)

      self.class.within_thread(batch_id: id, database: database) do
        buffer.capture(&block) if block
        buffer.prepare
      end

      validate_members(buffer.jobs)
      buffer
    end

    def enqueue_buffer(buffer)
      store.open(id)
      accepted = flush_members(buffer)
      store.mark_enqueued(id)
      request_reconciliation
      accepted
    end

    # Inserts an update row in this transaction, so a crash before the
    # after-commit reconcile still leaves work for {BatchStore#reconcile_pending}.
    def request_reconciliation
      database.with_connection { |connection| store.request(connection, [id]) }
      database.after_commit { store.reconcile(id) }
    end

    # Jobs with a provider ID are already stored. They are attached by UPDATE
    # instead of inserted again.
    def flush_members(buffer)
      existing = buffer.jobs.select(&:provider_job_id)

      self.class.within_thread(batch_id: id, database: database) do
        Bulk.with_buffer(nil) { buffer.enqueue }
        store.attach(id, existing) unless existing.empty?
      end

      buffer.jobs
    end
  end
end
