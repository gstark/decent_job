# frozen_string_literal: true

require "decent_job/support"

module DecentJob
  # Extensions shared by native and Active Job classes. They use only the API
  # both frameworks provide: callbacks, retry_on, serialize and deserialize.
  module JobExtensions
    # Searchable labels on jobs. {Base} includes it. Active Job classes get it
    # through ActiveJobExtensions::Labels.
    #
    # The class setting decent_job_labels holds defaults: strings, or procs
    # that run with instance_exec on the job. Instance labels from
    # set(decent_job_labels:) or the writer replace the class defaults.
    # Labels are trimmed, converted to strings, and deduplicated. The payload
    # key "decent_job_labels" feeds the decent_jobs.labels column, which
    # pauses and concurrency label rules read.
    #
    # See the "Labels, concurrency, and pauses" section of README.md.
    module Labels
      def self.included(base)
        Support.class_setting(base, :decent_job_labels, [])
        base.prepend(Serialization)
      end

      # Prepended so it wraps the framework's enqueue and payload methods.
      module Serialization
        # @param options [Hash] enqueue options. :decent_job_labels replaces
        #   the labels before the callbacks run.
        def enqueue(options = {})
          self.decent_job_labels = options[:decent_job_labels] if options.key?(:decent_job_labels)
          super
        end

        # @return [Hash{String => Object}] the payload with "decent_job_labels"
        def serialize
          super.merge("decent_job_labels" => decent_job_labels.dup)
        end

        # Stored labels replace the class defaults. A payload without the key
        # gives an empty list.
        def deserialize(job_data)
          super
          self.decent_job_labels = job_data["decent_job_labels"]
        end
      end

      # Returns the job labels.
      #
      # Jobs are constructed without arguments when deserialized. Resolve
      # defaults on first access, after arguments are available.
      #
      # @return [Array<String>] the labels, plus labels that concurrency rules
      #   apply
      # @note Each call adds applied labels to the stored list in place, and
      #   can load serialized arguments.
      def decent_job_labels
        deserialize_arguments_if_needed
        @decent_job_labels ||= normalize_decent_job_labels(Array(self.class.decent_job_labels).map do |label|
          label.respond_to?(:call) ? instance_exec(&label) : label
        end)
        @decent_job_labels.replace(normalize_decent_job_labels(@decent_job_labels + Array(decent_job_applied_labels)))
      end

      # @param labels [Array<#to_s>, #to_s, nil] the labels; nil and blank
      #   values are dropped
      def decent_job_labels=(labels)
        @decent_job_labels = normalize_decent_job_labels(labels)
      end

      # Extension point for labels that other extensions add.
      #
      # @return [Array<String>]
      # @api private
      def decent_job_applied_labels
        []
      end

      private

      def normalize_decent_job_labels(labels)
        Array(labels).compact.map { |label| label.to_s.strip }.reject(&:empty?).uniq
      end
    end
  end
end
