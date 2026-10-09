# frozen_string_literal: true

require "decent_job/cron_schedule"

module DecentJob
  class CronEntry
    VALUE_TYPES = {args: Array, kwargs: Hash, set: Hash}.freeze

    attr_reader :params, :errors

    def self.all(configuration: nil)
      (configuration || DecentJob.configuration).cron_entries
    end

    def self.find(key, configuration: nil)
      all(configuration: configuration).find { |entry| entry.key.to_s == key.to_s } || raise(KeyError, "Cron entry #{key} was not found")
    end

    def self.current_context
      Thread.current.thread_variable_get(:decent_job_cron_context)
    end

    def self.with_context(context)
      previous = current_context
      Thread.current.thread_variable_set(:decent_job_cron_context, context)
      yield
    ensure
      Thread.current.thread_variable_set(:decent_job_cron_context, previous)
    end

    def self.context_for(database)
      context = current_context
      context if context && context.fetch(:database).equal?(database)
    end

    def self.validate_jobs(jobs)
      database = current_context.fetch(:database)
      jobs.each do |job|
        adapter = job.queue_adapter if job.is_a?(ActiveJob::Base)
        unless adapter.is_a?(Adapter) && adapter.database.equal?(database)
          raise ArgumentError, "Cron jobs must use a DecentJob adapter with the cron entry database"
        end
      end
    end

    def self.last_jobs_by_key(entries, before: nil)
      entries.group_by(&:database).each_with_object({}) do |(database, group), result|
        result.merge!(CronStore.new(database).last_jobs_by_key(group.map(&:key), before: before))
      end
    end

    def initialize(params = {}, database: nil, **attributes)
      @params = params.merge(attributes)
      @database = database
      @errors = {}
    end

    %i[key args kwargs set description cron].each do |name|
      define_method(name) { params[name] }
    end
    alias_method :id, :key
    alias_method :to_param, :key

    def job_class
      params[:class]
    end

    def valid?
      @errors = {}
      add_error(:key, "must be a Symbol") unless key.is_a?(Symbol)
      add_error(:cron, "is not a valid schedule") unless cron.respond_to?(:call) || schedule
      unless job_class.blank? || job_class.is_a?(Class) || job_class.respond_to?(:call)
        begin
          resolved = job_class.is_a?(String) && job_class.constantize
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

    def display_properties
      {key: key, class: display_property(job_class), cron: display_schedule,
       set: display_property(set), description: display_property(description)}.tap do |properties|
        properties[:args] = display_property(args) if args.present?
        properties[:kwargs] = display_property(kwargs) if kwargs.present?
      end
    end

    def display_schedule
      schedule ? schedule.original : display_property(cron)
    end

    def next_at(previously_at: nil, after: nil)
      value = cron.respond_to?(:call) ? cron.call(previously_at || last_job_at) : cron
      return value if cron.respond_to?(:call) && value.is_a?(Time)

      CronSchedule.parse(value)&.next_at(after: after)
    end

    def within(period, previously_at: nil)
      value = cron.respond_to?(:call) ? cron.call(previously_at || last_job_at) : cron
      CronSchedule.parse(value)&.within(period) || []
    end

    def enabled_by_default?
      value = params.fetch(:enabled_by_default, true)
      value.respond_to?(:call) ? value.call : value
    end

    def database
      @database || Current.execution&.fetch(:database, nil) || self.class.current_context&.fetch(:database, nil) || Batch.current_context&.fetch(:database, nil) || DecentJob.database
    end

    def enabled?
      store.enabled?(key, default: enabled_by_default?)
    end

    def enable
      store.set_enabled(key, true)
    end

    def disable
      store.set_enabled(key, false)
    end

    def jobs
      store.jobs(key)
    end

    def last_job
      store.last_jobs_by_key([key])[key.to_s]
    end

    def last_job_at
      row = last_job
      (row["cron_at"] || row["created_at"]).localtime if row
    end

    def enqueue(cron_at = nil)
      raise ArgumentError, "cron_at must be a Time or nil" unless cron_at.nil? || cron_at.is_a?(Time)

      connection_pool = database
      buffer = Bulk::Buffer.new
      result = nil
      attempted = false
      Bulk.with_buffer(nil) do
        connection_pool.transaction do
          self.class.with_context(key: key, at: cron_at, database: connection_pool) do
            I18n.with_locale(I18n.default_locale) do
              buffer.capture { result = enqueue_definition }
              buffer.prepare
              self.class.validate_jobs(buffer.active_jobs)
              ConcurrencyControls.new(connection_pool).lock_scopes(buffer.active_jobs)
              attempted = !buffer.active_jobs.empty?
              pending = buffer.active_jobs.reject(&:provider_job_id)
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
      if result.is_a?(ActiveJob::Base)
        result.successfully_enqueued? ? result : false
      elsif result == false
        false
      else
        attempted ? buffer.active_jobs : nil
      end
    rescue
      buffer&.discard
      raise
    end

    private

    def store
      CronStore.new(database)
    end

    def enqueue_definition
      klass = resolve_value(job_class)
      return if klass.blank?
      klass = klass.constantize if klass.is_a?(String)
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
