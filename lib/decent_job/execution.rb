# frozen_string_literal: true

module DecentJob
  # Read-only attempt snapshots. Latencies and measured durations are seconds.
  class Execution
    ATTRIBUTES = %w[id job_id started_at finished_at state error process_id process_uuid batch_id error_event payload scheduled_at job_class queue_name duration].freeze
    attr_reader :database, :id
    (ATTRIBUTES - ["id"]).each { |name| define_method(name) { @attributes.fetch(name) } }
    alias_method :performed_at, :started_at

    def self.find(id, database: nil)
      database ||= Current.execution&.fetch(:database, nil) || DecentJob.database
      database.with_connection do |connection|
        row = connection.exec_params("SELECT * FROM decent_job_executions WHERE id = $1", [id]).first
        raise KeyError, "Execution #{id} was not found" unless row
        new(Store.new(database).decode(row), database: database)
      end
    end

    def self.for_job(job_id, database: nil)
      database ||= Current.execution&.fetch(:database, nil) || DecentJob.database
      Store.new(database).executions(job_id).map { |attributes| new(attributes, database: database) }
    end

    def initialize(attributes, database:)
      @attributes = Support.deep_dup(attributes)
      @id = @attributes.fetch("id").to_s.dup.freeze
      @database = database
    end

    def attributes
      Support.deep_dup(@attributes)
    end

    def number
      payload.fetch("executions", 0) + 1 if payload
    end

    def queue_latency
      started_at - scheduled_at if started_at && scheduled_at
    end

    def runtime_latency
      duration
    end

    def total_latency
      queue = queue_latency
      runtime = runtime_latency
      queue + runtime if queue && runtime
    end

    def interrupted_duration
      finished_at - started_at if error_event == "interrupted" && finished_at && started_at
    end

    def last_status_at
      finished_at || started_at
    end

    def status
      state.to_sym
    end
  end
end
