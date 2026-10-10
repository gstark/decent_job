# frozen_string_literal: true

module DecentJob
  # Reads and writes persistent pauses in decent_job_pauses.
  #
  # A pause targets one queue, one job class, or one label. Pauses persist
  # until they are removed. Claim queries exclude paused jobs only when
  # {Configuration#enable_pauses} is true; this class reads and writes pauses
  # in either case. A pause stops new claims. It does not interrupt running
  # jobs.
  #
  # The top-level {DecentJob.pause} methods, {Adapter}, and {Dashboard} create
  # short-lived instances. An instance holds only the {Database}.
  #
  # See the "Labels, concurrency, and pauses" section of README.md and
  # docs/job-management.md.
  class Pauses
    # Only interpolate this expression with the library's fixed jobs table alias.
    # It is true when no pause matches the job's queue_name, job_class, or
    # any of its labels.
    EXCLUDED_SQL = <<~SQL
      NOT EXISTS (
        SELECT 1 FROM decent_job_pauses
        WHERE (kind = 'queues' AND key = decent_jobs.queue_name)
          OR (kind = 'job_classes' AND key = decent_jobs.job_class)
          OR (kind = 'labels' AND key = ANY(decent_jobs.labels))
      )
    SQL

    # @param database [Database] the pool for all queries
    def initialize(database)
      @database = database
    end

    # Adds a pause. Pausing a target twice has no effect.
    #
    # @param target [Hash] exactly one of queue:, job_class:, or label:. The
    #   value is converted with to_s and stripped.
    # @return [true]
    # @raise [ArgumentError] when there is not exactly one target, or it is blank
    # @note Sends no NOTIFY. Claim queries apply the pause on their next run.
    def pause(**target)
      kind, key = target_pair(**target)
      @database.with_connection do |connection|
        connection.exec_params("INSERT INTO decent_job_pauses (kind, key) VALUES ($1, $2) ON CONFLICT DO NOTHING", [kind, key])
      end
      true
    end

    # Removes a pause and wakes workers.
    #
    # @param target [Hash] exactly one of queue:, job_class:, or label:
    # @return [true] also when no matching pause existed
    # @raise [ArgumentError] when there is not exactly one target, or it is blank
    # @note Sends NOTIFY with an empty payload, which wakes all queues.
    def unpause(**target)
      kind, key = target_pair(**target)
      @database.transaction do |connection|
        connection.exec_params("DELETE FROM decent_job_pauses WHERE kind = $1 AND key = $2", [kind, key])
        Store.new(@database).notify(connection)
      end
      true
    end

    # @return [Hash{Symbol => Array<String>}] the paused keys in :queues,
    #   :job_classes, and :labels, each sorted by key
    def paused
      @database.with_connection do |connection|
        connection.exec("SELECT kind, key FROM decent_job_pauses ORDER BY key").each_with_object(queues: [], job_classes: [], labels: []) do |row, result|
          result.fetch(row.fetch("kind").to_sym) << row.fetch("key")
        end
      end
    end

    # Reports whether a job, a target, or anything is paused.
    #
    # @param job_instance [Object, nil] a job to check against all its targets
    # @param target [Hash] at most one of queue:, job_class:, or label:
    # @return [Symbol, Boolean] the {#reason} for job_instance. Without
    #   arguments, true when any pause exists. With a target, true when that
    #   target is paused.
    # @raise [ArgumentError] when job_instance and a target are both given, or
    #   more than one target is given
    def paused?(job_instance: nil, **target)
      if job_instance
        raise ArgumentError, "provide only one target" unless target.values.compact.empty?
        labels = job_instance.respond_to?(:decent_job_labels) ? job_instance.decent_job_labels : []
        reason(queue: job_instance.queue_name, job_class: job_instance.class.name, labels: labels)
      elsif target.values.compact.empty?
        paused.values.any?(&:any?)
      else
        kind, key = target_pair(**target)
        paused.fetch(kind.to_sym).include?(key)
      end
    end

    # Finds the first pause that matches a job. Queue pauses win over class
    # pauses, and class pauses win over label pauses.
    #
    # @param queue [String, Symbol] the queue name
    # @param job_class [String, Class] the job class or its name
    # @param labels [Array<String>] the job labels
    # @return [Symbol, false] :queue_name_paused, :job_class_paused,
    #   :label_paused, or false when nothing matches
    def reason(queue:, job_class:, labels: [])
      values = paused
      return :queue_name_paused if values.fetch(:queues).include?(queue.to_s)
      return :job_class_paused if values.fetch(:job_classes).include?(job_class.to_s)
      return :label_paused unless (Array(labels).map { |label| label.to_s.strip } & values.fetch(:labels)).empty?
      false
    end

    private

    def target_pair(queue: nil, job_class: nil, label: nil)
      pairs = {"queues" => queue, "job_classes" => job_class, "labels" => label}.reject { |_, value| value.nil? }
      raise ArgumentError, "provide exactly one of queue, job_class or label" unless pairs.size == 1
      kind, value = pairs.first
      key = value.to_s.strip
      raise ArgumentError, "pause target cannot be blank" if key.empty?
      [kind, key]
    end
  end
end
