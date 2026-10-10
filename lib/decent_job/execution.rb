# frozen_string_literal: true

module DecentJob
  # A read-only snapshot of one attempt, from a decent_job_executions row.
  #
  # {Execution.find} and {Execution.for_job} build snapshots for application
  # code. {Performer} builds one from the claim row for the perform_job
  # instrumentation payload. The object holds a deep copy of the decoded row
  # and never reloads. Look it up again to see a running attempt finish.
  # Reading a snapshot never loads or deserializes the job class.
  #
  # Latencies and measured durations are seconds. A value is nil when it is
  # unknown, for example for attempts that a killed process abandoned, or
  # rows from before migration 008.
  #
  # See docs/execution-history.md.
  class Execution
    # Column names of decent_job_executions that the snapshot exposes.
    # process_id is the OS process ID and process_uuid the process record. Each
    # name except id gets a reader with the decoded value.
    ATTRIBUTES = %w[id job_id started_at finished_at state error process_id process_uuid batch_id error_event payload scheduled_at job_class queue_name duration].freeze

    # +database+ is the {Database} the snapshot came from. +id+ is the frozen
    # execution UUID String.
    attr_reader :database, :id
    (ATTRIBUTES - ["id"]).each { |name| define_method(name) { @attributes.fetch(name) } }
    alias_method :performed_at, :started_at

    # Loads one attempt by ID.
    #
    # @param id [String] the decent_job_executions.id
    # @param database [Database, nil] nil uses the database of the current
    #   execution, then the global database
    # @return [Execution]
    # @raise [KeyError] if no row has this ID
    def self.find(id, database: nil)
      database ||= Current.execution&.fetch(:database, nil) || DecentJob.database

      database.with_connection do |connection|
        row = connection.exec_params("SELECT * FROM decent_job_executions WHERE id = $1", [id]).first
        raise KeyError, "Execution #{id} was not found" unless row

        new(Store.new(database).decode(row), database: database)
      end
    end

    # Loads every retained attempt of one job, ordered by started_at and id.
    #
    # @param job_id [String] the decent_jobs.id (provider job ID)
    # @param database [Database, nil] see {.find}
    # @return [Array<Execution>] empty when the job has no retained attempts
    def self.for_job(job_id, database: nil)
      database ||= Current.execution&.fetch(:database, nil) || DecentJob.database
      Store.new(database).executions(job_id).map { |attributes| new(attributes, database: database) }
    end

    # @param attributes [Hash{String => Object}] a row decoded by {Store#decode}
    # @param database [Database]
    def initialize(attributes, database:)
      @attributes = Support.deep_dup(attributes)
      @id = @attributes.fetch("id").to_s.dup.freeze
      @database = database
    end

    # @return [Hash{String => Object}] a deep copy of the decoded row
    def attributes
      Support.deep_dup(@attributes)
    end

    # @return [Integer, nil] the Active Job execution number from the payload
    #   snapshot, or nil when the payload is unknown. Backend retries keep the
    #   payload, so two attempts can share a number.
    def number
      payload.fetch("executions", 0) + 1 if payload
    end

    # @return [Float, nil] seconds from scheduled_at to started_at, from wall
    #   timestamps. Can be negative after clock changes.
    def queue_latency
      started_at - scheduled_at if started_at && scheduled_at
    end

    # @return [Float, nil] the monotonic duration in seconds, or nil until the
    #   attempt completes normally
    def runtime_latency
      duration
    end

    # @return [Float, nil] queue latency plus runtime, or nil if either is unknown
    def total_latency
      queue = queue_latency
      runtime = runtime_latency
      queue + runtime if queue && runtime
    end

    # @return [Float, nil] wall seconds from start until recovery detected the
    #   interruption, including any downtime. Nil unless error_event is
    #   "interrupted".
    def interrupted_duration
      finished_at - started_at if error_event == "interrupted" && finished_at && started_at
    end

    # @return [Time, nil] finished_at, or started_at while unfinished
    def last_status_at
      finished_at || started_at
    end

    # @return [Symbol] the attempt state: :running, :succeeded, :discarded,
    #   :retried, or :interrupted
    def status
      state.to_sym
    end
  end
end
