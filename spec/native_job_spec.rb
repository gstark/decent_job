# frozen_string_literal: true

require "open3"
require "rbconfig"
require "spec_helper"

# Records enqueues in memory so these specs cover the job layer alone.
class RecordingAdapter
  attr_reader :jobs, :bulk
  attr_accessor :error

  def initialize
    @jobs = []
    @bulk = []
  end

  def enqueue(job)
    raise error if error

    job.provider_job_id = "provider-#{@jobs.size}"
    @jobs << [job, nil]
  end

  def enqueue_at(job, timestamp)
    raise error if error

    @jobs << [job, timestamp]
  end

  def enqueue_all(jobs)
    @bulk.concat(jobs)
    jobs.size
  end
end

class NativeLogJob < DecentJob::Base
  def self.log = (@log ||= [])

  def perform(value, keyword:)
    self.class.log << [:perform, value, keyword]
    value
  end
end

describe DecentJob::Base do
  before do
    @adapter = RecordingAdapter.new
    DecentJob::Base.queue_adapter = @adapter
    NativeLogJob.log.clear
  end

  after do
    DecentJob::Base.remove_instance_variable(:@queue_adapter)
  end

  def job_class(&block)
    Class.new(DecentJob::Base) do
      def self.name = "AnonymousNativeJob"

      def self.log = (@log ||= [])

      class_eval(&block)
    end
  end

  describe "callbacks" do
    it "nests mixed callbacks in definition order like Active Job" do
      klass = job_class do
        before_perform { self.class.log << :b1 }

        around_perform do |_job, block|
          self.class.log << :a1_in
          block.call
          self.class.log << :a1_out
        end

        after_perform { self.class.log << :af1 }
        before_perform { self.class.log << :b2 }

        around_perform do |_job, block|
          self.class.log << :a2_in
          block.call
          self.class.log << :a2_out
        end

        after_perform { self.class.log << :af2 }
        define_method(:perform) { self.class.log << :perform }
      end

      klass.perform_now
      _(klass.log).must_equal %i[b1 a1_in b2 a2_in perform af2 a2_out af1 a1_out]
    end

    it "halts on :abort and skips the rest of the chain" do
      klass = job_class do
        after_perform { self.class.log << :after }
        before_perform { throw :abort }
        define_method(:perform) { self.class.log << :perform }
      end

      _(klass.perform_now).must_equal false
      _(klass.log).must_equal []
    end

    it "returns nil when an around callback does not yield" do
      klass = job_class do
        around_perform { |_job, _block| self.class.log << :around }
        define_method(:perform) { self.class.log << :perform }
      end

      _(klass.perform_now).must_be_nil
      _(klass.log).must_equal [:around]
    end

    it "evaluates method names and if/unless conditions on the job" do
      klass = job_class do
        before_perform :record, if: -> { arguments.first }
        before_perform(unless: :skip?) { self.class.log << :unless }
        define_method(:record) { self.class.log << :record }
        define_method(:skip?) { arguments.first == :skip }
        define_method(:perform) { |*| 42 }
      end

      _(klass.perform_now(true)).must_equal 42
      _(klass.log).must_equal %i[record unless]
      klass.log.clear
      klass.perform_now(:skip)
      _(klass.log).must_equal [:record]
    end

    it "applies parent callbacks added after a subclass exists" do
      parent = job_class { define_method(:perform) { self.class.log << :perform } }
      child = Class.new(parent)
      parent.before_perform { self.class.log << :parent }
      child.perform_now
      _(child.log).must_equal %i[parent perform]
    end
  end

  describe "enqueue" do
    it "enqueues now or later and returns the job" do
      job = NativeLogJob.perform_later(1, keyword: 2)
      _(job).must_be_kind_of NativeLogJob
      _(job.successfully_enqueued?).must_equal true
      _(@adapter.jobs).must_equal [[job, nil]]

      at = Time.now + 60
      later = NativeLogJob.set(wait_until: at, queue: :urgent, priority: "3").perform_later(1, keyword: 2)
      _(@adapter.jobs.last).must_equal [later, at.to_f]
      _(later.queue_name).must_equal "urgent"
      _(later.priority).must_equal 3

      waited = NativeLogJob.set(wait: 30).perform_later(1, keyword: 2)
      _(waited.scheduled_at).must_be_close_to Time.now + 30, 1
    end

    it "yields the job and passes keyword arguments to perform" do
      yielded = nil
      job = NativeLogJob.perform_later(5, keyword: :k) { |enqueued| yielded = enqueued }
      _(yielded).must_be_same_as job
      restored = DecentJob::Base.deserialize(JSON.parse(JSON.generate(job.serialize)))
      _(restored.perform_now).must_equal 5
      _(NativeLogJob.log).must_equal [[:perform, 5, :k]]
      _(restored.executions).must_equal 1
    end

    it "returns false when a before_enqueue callback aborts or the adapter rejects" do
      klass = job_class do
        before_enqueue { throw :abort if arguments.first == :abort }
        define_method(:perform) { |*| }
      end

      _(klass.perform_later(:abort)).must_equal false
      _(@adapter.jobs).must_equal []

      @adapter.error = DecentJob::EnqueueError.new("rejected")
      job = klass.new(:ok)
      _(job.enqueue).must_equal false
      _(job.enqueue_error.message).must_equal "rejected"
    end

    it "bulk enqueues through each adapter without callbacks" do
      klass = job_class do
        before_enqueue { raise "callbacks must not run" }
        define_method(:perform) { |*| }
      end

      jobs = [klass.new(1), klass.new(2)]
      _(DecentJob.perform_all_later(jobs)).must_be_nil
      _(@adapter.bulk).must_equal jobs
    end
  end

  describe "queue settings" do
    it "resolves queue names, prefixes and priorities through inheritance" do
      parent = job_class do
        queue_as :reports
        queue_with_priority 10
      end

      child = Class.new(parent)
      _(child.new.queue_name).must_equal "reports"
      _(child.new.priority).must_equal 10
      child.queue_name_prefix = "staging"
      child.queue_as { arguments.first }
      _(child.new(:dynamic).queue_name).must_equal "staging_dynamic"
      _(parent.new.queue_name).must_equal "reports"
      _(job_class {}.new.queue_name).must_equal "default"
    end
  end

  describe "error handling" do
    it "retries with the configured delay and stops after the attempt limit" do
      klass = job_class do
        retry_on ArgumentError, wait: 10, attempts: 2
        define_method(:perform) { raise ArgumentError, "flaky" }
      end

      job = klass.new
      _(job.perform_now).must_be_kind_of ArgumentError
      _(job.decent_job_outcome.first).must_equal :retried
      _(@adapter.jobs.size).must_equal 1
      _(@adapter.jobs.last.last).must_be_close_to Time.now.to_f + 10, 1
      _(job.scheduled_at).must_be_nil
      _(job.exception_executions).must_equal("[ArgumentError]" => 1)

      _ { job.perform_now }.must_raise ArgumentError
      _(job.decent_job_outcome.first).must_equal :retry_stopped
      _(@adapter.jobs.size).must_equal 1
    end

    it "calls the exhausted block with the class as self" do
      seen = nil

      klass = job_class do
        retry_on(ArgumentError, attempts: 1) { |job, error| seen = [self, job.class, error.message] }
        define_method(:perform) { raise ArgumentError, "done" }
      end

      _(klass.perform_now).must_be_kind_of ArgumentError
      _(seen).must_equal [klass, klass, "done"]
    end

    it "computes polynomial and proc delays" do
      klass = job_class do
        retry_on ArgumentError, wait: :polynomially_longer
        retry_on KeyError, wait: ->(executions) { executions * 100 }
        define_method(:perform) { |error| raise error }
      end

      klass.perform_now(ArgumentError)
      _(@adapter.jobs.last.last).must_be_close_to Time.now.to_f + 3, 1
      klass.perform_now(KeyError)
      _(@adapter.jobs.last.last).must_be_close_to Time.now.to_f + 100, 1
    end

    it "discards, runs after_discard and falls back to the cause" do
      klass = job_class do
        discard_on KeyError
        after_discard { |_job, error| self.class.log << [:after_discard, error.class] }

        define_method(:perform) do
          raise KeyError
        rescue KeyError
          raise "wrapped"
        end
      end

      job = klass.new
      _(job.perform_now).must_be_kind_of KeyError
      _(job.decent_job_outcome.first).must_equal :discarded
      _(klass.log).must_equal [[:after_discard, KeyError]]
    end

    it "uses the last matching rescue_from handler and runs after_discard for unhandled errors" do
      klass = job_class do
        rescue_from(StandardError) { |error| self.class.log << [:standard, error.message] }
        rescue_from("ArgumentError", with: :argument)
        after_discard { |_job, error| self.class.log << [:after_discard, error.message] }
        define_method(:argument) { |error| self.class.log << [:argument, error.message] }
        define_method(:perform) { |error| raise error }
      end

      klass.perform_now(ArgumentError.new("a"))
      klass.perform_now(RuntimeError.new("b"))
      _ { klass.perform_now(Exception.new("c")) }.must_raise Exception
      _(klass.log).must_equal [[:argument, "a"], [:standard, "b"], [:after_discard, "c"]]
    end
  end

  it "performs with callbacks and retries without Active Job or ActiveSupport" do
    script = <<~RUBY
      require "decent_job"
      raise "Active Job loaded" if defined?(ActiveJob)
      raise "ActiveSupport loaded" if defined?(ActiveSupport)

      class Adapter
        attr_reader :timestamps
        def enqueue_at(job, timestamp) = (@timestamps ||= []) << timestamp
      end

      class FlakyJob < DecentJob::Base
        self.queue_adapter = Adapter.new
        retry_on RuntimeError, wait: 5
        around_perform { |_job, block| block.call }
        def perform(at:) = raise("flaky \#{at.year}")
      end

      job = FlakyJob.new(at: Time.utc(2026))
      payload = JSON.parse(JSON.generate(job.serialize))
      restored = DecentJob::Base.deserialize(payload)
      raise "not handled" unless restored.perform_now.message == "flaky 2026"
      raise "not retried" unless restored.decent_job_outcome.first == :retried
      raise "not scheduled" unless FlakyJob.queue_adapter.timestamps.size == 1
    RUBY
    output, status = Open3.capture2e(RbConfig.ruby, "-Ilib", "-rjson", "-e", script)
    _(status.success?).must_equal true, output
  end

  describe "persistence" do
    include DatabaseSpec

    it "stores a native job through the DecentJob adapter" do
      DecentJob::Schema.new(database).migrate
      DecentJob::Base.queue_adapter = DecentJob::Adapter.new(database: database)
      at = Time.now.utc + 3600
      job = NativeLogJob.set(queue: :mail, priority: 4, wait_until: at).perform_later(:a, keyword: {b: 1})
      row = DecentJob::Store.new(database).find(job.provider_job_id)
      _(row.values_at("job_class", "active_job_id", "queue_name", "priority", "state")).must_equal ["NativeLogJob", job.job_id, "mail", 4, "ready"]
      _(row.fetch("scheduled_at").to_f).must_be_close_to at.to_f, 0.001
      restored = DecentJob::Base.deserialize(row.fetch("payload"))
      restored.send(:deserialize_arguments_if_needed)
      _(restored.arguments).must_equal [:a, {keyword: {b: 1}}]
    end
  end
end
