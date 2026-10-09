# frozen_string_literal: true

require "decent_job/batch_properties"
require "decent_job/bulk"

module DecentJob
  class Batch
    include GlobalID::Identification

    ATTRIBUTES = %i[description on_finish on_success on_discard callback_queue_name callback_priority].freeze
    TIMESTAMPS = %i[created_at updated_at enqueued_at jobs_finished_at finished_at discarded_at].freeze

    attr_reader :database

    def self.default_database
      Current.execution&.fetch(:database, nil) || current_context&.fetch(:database, nil) || DecentJob.database
    end

    def self.enqueue(jobs = [], database: nil, **properties, &block)
      new(database: database, **properties).tap { |batch| batch.enqueue(jobs, &block) }
    end

    def self.find(id, database: nil)
      database ||= default_database
      row = BatchStore.new(database).find(id)
      raise KeyError, "Batch #{id} was not found" unless row

      new(database: database, record: row)
    end

    def self.where(id:, database: nil)
      database ||= default_database
      Array(id).filter_map do |value|
        row = BatchStore.new(database).find(value)
        new(database: database, record: row) if row
      end
    end

    def self.primary_key
      :id
    end

    def self.current_context
      ExecutionState[:batch_context]
    end

    def self.context_for(database, job: nil)
      context = current_context
      return unless context && context[:database].equal?(database)

      batch_id = (context[:batch_ids] && job) ? context[:batch_ids][job.job_id] : context[:batch_id]
      {batch_id: batch_id, batch_callback_id: context[:batch_callback_id]}
    end

    def self.current_batch_id
      current_context&.fetch(:batch_id)
    end

    def self.current_batch_callback_id
      current_context&.fetch(:batch_callback_id)
    end

    def self.within_thread(batch_id: nil, batch_callback_id: nil, batch_ids: nil, database: nil)
      database ||= default_database
      database.transaction do
        with_context(batch_id: batch_id, batch_callback_id: batch_callback_id, batch_ids: batch_ids, database: database) { yield }
      end
    end

    def self.with_context(context)
      ExecutionState.with(:batch_context, context) { yield }
    end

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
            batch_ids = member_batches(batches.zip(buffers.map(&:active_jobs))).transform_values(&:id)
            existing_members = buffers.map { |buffer| buffer.active_jobs.select(&:provider_job_id) }
            store.lock_scopes(buffers.flat_map(&:active_jobs))
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

    def initialize(database: nil, record: nil, **properties)
      @database = database || self.class.default_database
      @row = record ? record.dup : {"properties" => BatchProperties.dump({})}
      @properties_loaded = false
      assign_properties(properties)
    end

    ATTRIBUTES.each do |name|
      define_method(name) { @row[name.to_s] }
      define_method("#{name}=") do |value|
        @row[name.to_s] = (name.to_s.start_with?("on_") && value.is_a?(Class)) ? value.name : value
      end
    end
    TIMESTAMPS.each { |name| define_method(name) { @row[name.to_s] } }

    def id
      @row["id"]
    end

    def persisted?
      !id.nil?
    end

    %i[enqueued jobs_finished finished discarded].each do |name|
      define_method("#{name}?") { !public_send("#{name}_at").nil? }
    end

    def succeeded?
      finished? && !discarded?
    end

    def properties
      unless @properties_loaded
        @properties = Current.with(database: database) { BatchProperties.load(@row.fetch("properties")) }
        @properties_loaded = true
      end
      @properties
    end

    def properties=(value)
      raise ArgumentError, "Properties must be a Hash" unless value.is_a?(Hash)

      @properties = value.dup
      @properties_loaded = true
    end

    def assign_properties(attributes)
      attributes = attributes.dup
      self.properties = attributes.delete(:properties) if attributes.key?(:properties)
      ATTRIBUTES.each { |name| public_send("#{name}=", attributes.delete(name)) if attributes.key?(name) }
      properties.merge!(attributes) unless attributes.empty?
      self
    end

    def save
      attributes = serialized_attributes
      if persisted?
        store.locked([id]) { @row = store.save(id: id, attributes: attributes) }
      else
        @row = store.save(id: nil, attributes: attributes)
      end
      true
    end

    def reload
      @row = store.find(id) || raise(KeyError, "Batch #{id} was not found")
      @properties_loaded = false
      self
    end

    def to_global_id(options = {})
      super({app: "decent-job"}.merge(options))
    end

    def display_attributes
      properties = Current.with(database: database) { BatchProperties.display(@row.fetch("properties")) }
      @row.merge("properties" => properties)
    end

    def enqueue(jobs = [], **properties, &block)
      assign_properties(properties)
      validate_members(Array(jobs).compact)
      accepted = transaction do
        save_and_lock do
          buffer = prepare_members(jobs, &block)
          store.lock_scopes(buffer.active_jobs)
          enqueue_buffer(buffer)
        end
      end
      reload
      accepted
    end

    def add(jobs = [], &block)
      validate_members(Array(jobs).compact)
      accepted = transaction do
        save_and_lock do
          buffer = prepare_members(jobs, &block)
          store.lock_scopes(buffer.active_jobs)
          flush_members(buffer)
        end
      end
      reload
      accepted
    end

    def active_jobs
      persisted? ? store.members(id) : []
    end

    def callback_active_jobs
      persisted? ? store.members(id, callbacks: true) : []
    end

    def retry
      transaction do
        save unless persisted?
        store.locked([id]) do
          store.open(id)
          ids = store.retry_members(id)
          store.mark_enqueued(id)
          request_reconciliation
          inline_jobs = store.members(id, ids: ids).select { |job| job.queue_adapter.execution_mode == :inline }
          database.after_commit { InlineBuffer.perform(database, inline_jobs.map(&:provider_job_id)) } unless inline_jobs.empty?
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
        unless job.is_a?(ActiveJob::Base) && job.queue_adapter.is_a?(Adapter) && job.queue_adapter.database.equal?(database)
          raise ArgumentError, "Batch jobs must use a DecentJob adapter with the batch database"
        end
      end
    end

    def prepare_members(jobs, &block)
      buffer = Bulk::Buffer.new
      buffer.add(Array(jobs).compact)
      self.class.within_thread(batch_id: id, database: database) do
        buffer.capture(&block) if block
        buffer.prepare
      end
      validate_members(buffer.active_jobs)
      buffer
    end

    def enqueue_buffer(buffer)
      store.open(id)
      accepted = flush_members(buffer)
      store.mark_enqueued(id)
      request_reconciliation
      accepted
    end

    def request_reconciliation
      database.with_connection { |connection| store.request(connection, [id]) }
      database.after_commit { store.reconcile(id) }
    end

    def flush_members(buffer)
      existing = buffer.active_jobs.select(&:provider_job_id)
      self.class.within_thread(batch_id: id, database: database) do
        Bulk.with_buffer(nil) { buffer.enqueue }
        store.attach(id, existing) unless existing.empty?
      end
      buffer.active_jobs
    end
  end
end
