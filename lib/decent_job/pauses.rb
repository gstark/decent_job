# frozen_string_literal: true

module DecentJob
  class Pauses
    # Only interpolate this expression with the library's fixed jobs table alias.
    EXCLUDED_SQL = <<~SQL
      NOT EXISTS (
        SELECT 1 FROM decent_job_pauses
        WHERE (kind = 'queues' AND key = decent_jobs.queue_name)
          OR (kind = 'job_classes' AND key = decent_jobs.job_class)
          OR (kind = 'labels' AND key = ANY(decent_jobs.labels))
      )
    SQL

    def initialize(database)
      @database = database
    end

    def pause(**target)
      kind, key = target_pair(**target)
      @database.with_connection do |connection|
        connection.exec_params("INSERT INTO decent_job_pauses (kind, key) VALUES ($1, $2) ON CONFLICT DO NOTHING", [kind, key])
      end
      true
    end

    def unpause(**target)
      kind, key = target_pair(**target)
      @database.transaction do |connection|
        connection.exec_params("DELETE FROM decent_job_pauses WHERE kind = $1 AND key = $2", [kind, key])
        Store.new(@database).notify(connection)
      end
      true
    end

    def paused
      @database.with_connection do |connection|
        connection.exec("SELECT kind, key FROM decent_job_pauses ORDER BY key").each_with_object(queues: [], job_classes: [], labels: []) do |row, result|
          result.fetch(row.fetch("kind").to_sym) << row.fetch("key")
        end
      end
    end

    def paused?(active_job: nil, **target)
      if active_job
        raise ArgumentError, "provide only one target" unless target.values.compact.empty?
        labels = active_job.respond_to?(:decent_job_labels) ? active_job.decent_job_labels : []
        reason(queue: active_job.queue_name, job_class: active_job.class.name, labels: labels)
      elsif target.values.compact.empty?
        paused.values.any?(&:any?)
      else
        kind, key = target_pair(**target)
        paused.fetch(kind.to_sym).include?(key)
      end
    end

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
