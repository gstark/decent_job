# frozen_string_literal: true

require "rbconfig"
require "spec_helper"
require "support/jobs"
require "timeout"

class WorkflowMemberJob < RecordingJob
end

class WorkflowCallbackJob < ActiveJob::Base
  class_attribute :events, default: Queue.new

  def perform(batch, options)
    self.class.events << [batch.id, options.fetch(:event), batch.properties]
  end
end

class WorkflowRetryCallbackJob < WorkflowCallbackJob
  retry_on RuntimeError, wait: 0, attempts: 2

  def perform(*)
    raise "first callback attempt" if executions == 1
    super
  end
end

class WorkflowDiscardOnceJob < WorkflowMemberJob
  discard_on ArgumentError

  def perform(value)
    raise ArgumentError, "discard first attempt" if executions == 1
    super
  end
end

class WorkflowRecursiveJob < WorkflowCallbackJob
  def perform(batch, options)
    if batch.properties.fetch(:stage) < 3
      batch.enqueue(stage: batch.properties.fetch(:stage) + 1) do
        2.times { WorkflowMemberJob.perform_later("stage member") }
      end
    end

    super
  end
end

class WorkflowAttachingJob < WorkflowMemberJob
  def perform(value)
    created = DecentJob::Batch.enqueue([self], database: DecentJob::Current.execution.fetch(:database), on_finish: WorkflowCallbackJob) do
      WorkflowMemberJob.perform_later("child")
    end

    self.class.events << [value, created.id, batch.id]
  end
end

class WorkflowChildJob < ActiveJob::Base
  def perform
    WorkflowMemberJob.perform_later("unassociated child")
  end
end

class WorkflowBlockingJob < BlockingJob
end

class WorkflowLimitedCallbackJob < WorkflowCallbackJob
  include DecentJob::ActiveJobExtensions::Concurrency
end

class WorkflowEnqueuingCallbackJob < WorkflowCallbackJob
  before_enqueue { WorkflowMemberJob.perform_later("callback child") }
end

class WorkflowControlledBulkChildJob < WorkflowMemberJob
  include DecentJob::ActiveJobExtensions::Concurrency

  class_attribute :enqueue_events, default: []

  before_enqueue do
    enqueue_events << [:before, job_id]
    self.queue_name = "callback-child"
    self.decent_job_labels = ["callback-child"]
  end

  after_enqueue { enqueue_events << [:after, job_id] }
end

class WorkflowBulkChildrenCallbackJob < WorkflowCallbackJob
  class_attribute :enqueue_events, default: []

  before_enqueue do
    enqueue_events << :before
    ActiveJob.perform_all_later(WorkflowControlledBulkChildJob.new("first child"), WorkflowControlledBulkChildJob.new("second child"))
  end

  after_enqueue { enqueue_events << :after }
end

class WorkflowFailOnceCallbackJob < WorkflowCallbackJob
  class_attribute :attempts, default: 0

  def perform(*)
    self.class.attempts += 1
    raise "first callback failed" if self.class.attempts == 1
    super
  end
end

class WorkflowKeyCallbackJob < WorkflowCallbackJob
  include DecentJob::ActiveJobExtensions::Concurrency

  decent_job_control_concurrency_with(key: -> { arguments.first.properties.fetch(:keys).fetch(arguments.last.fetch(:event)) }, total_limit: 100)
end

describe "batch workflows" do
  include DatabaseSpec

  before do
    DecentJob::Schema.new(database).migrate
    @adapter = DecentJob::Adapter.new(database: database)
    @classes = [WorkflowMemberJob, WorkflowCallbackJob, WorkflowRetryCallbackJob, WorkflowDiscardOnceJob,
      WorkflowRecursiveJob, WorkflowAttachingJob, WorkflowChildJob, WorkflowBlockingJob, WorkflowLimitedCallbackJob,
      WorkflowEnqueuingCallbackJob, WorkflowFailOnceCallbackJob, WorkflowKeyCallbackJob,
      WorkflowControlledBulkChildJob, WorkflowBulkChildrenCallbackJob]
    @classes.each { |klass| klass.queue_adapter = @adapter }
    WorkflowMemberJob.events = Queue.new
    WorkflowCallbackJob.events = Queue.new
    WorkflowBlockingJob.started = Queue.new
    WorkflowBlockingJob.release = Queue.new
    WorkflowLimitedCallbackJob.decent_job_concurrency_config = {}
    WorkflowLimitedCallbackJob.decent_job_concurrency_rules = []
    WorkflowFailOnceCallbackJob.attempts = 0
    WorkflowControlledBulkChildJob.enqueue_events = []
    WorkflowControlledBulkChildJob.decent_job_concurrency_config = {}
    WorkflowControlledBulkChildJob.decent_job_concurrency_rules = []
    WorkflowBulkChildrenCallbackJob.enqueue_events = []
    @performer = DecentJob::Performer.new(database: database)
    @threads = []
  end

  after do
    10.times { WorkflowBlockingJob.release << true }
    @threads.each(&:value)
  end

  def drain
    count = 0

    while @performer.perform_one
      count += 1
      raise "batch execution loop" if count > 100
    end

    count
  end

  def callback_events
    values = []
    values << WorkflowCallbackJob.events.pop until WorkflowCallbackJob.events.empty?
    values
  end

  def complete_during_reconciliation(batch, job)
    entered = Queue.new
    release = Queue.new
    registry = DecentJob::BatchStore.new(database)
    first_read = true

    registry.define_singleton_method(:find) do |id|
      if first_read
        first_read = false
        entered << true
        release.pop
      end

      super(id)
    end

    @threads << Thread.new { registry.reconcile(batch.id) }
    Timeout.timeout(5) { entered.pop }
    @threads << Thread.new { @performer.perform_one(job_id: job.provider_job_id) }
    store = DecentJob::Store.new(database)
    Timeout.timeout(5) { sleep 0.001 until store.find(job.provider_job_id).fetch("state") == "discarded" }
    release << true
    Timeout.timeout(5) { @threads.each(&:value) }
  ensure
    release << true
  end

  it "distinguishes member completion from callback completion" do
    batch = DecentJob::Batch.enqueue([WorkflowMemberJob.new("member")], database: database,
      on_success: WorkflowCallbackJob, on_finish: WorkflowCallbackJob,
      callback_queue_name: "callbacks", callback_priority: -5, account: 42)
    _(@performer.perform_one.state).must_equal "succeeded"
    _(batch.reload.jobs_finished?).must_equal true
    _(batch.finished?).must_equal false
    _(batch.callback_job_instances.size).must_equal 2
    _(batch.callback_job_instances.map { |job| [job.queue_name, job.priority] }).must_equal [["callbacks", -5], ["callbacks", -5]]
    drain
    _(batch.reload.succeeded?).must_equal true
    _(callback_events.map { |_, event, _| event }.sort).must_equal [:finish, :success]
    _(batch.properties).must_equal({account: 42})
  end

  it "runs discard and finish callbacks and allows callback retries" do
    batch = DecentJob::Batch.enqueue([WorkflowDiscardOnceJob.new("member")], database: database,
      on_success: WorkflowRetryCallbackJob, on_discard: WorkflowRetryCallbackJob, on_finish: WorkflowRetryCallbackJob)
    drain
    _(batch.reload.discarded?).must_equal true
    _(batch.finished?).must_equal true
    _(callback_events.map { |_, event, _| event }.sort).must_equal [:discard, :finish]
    counts = batch.callback_job_instances.map { |job| DecentJob::Store.new(database).executions(job.provider_job_id).size }
    _(counts).must_equal [2, 2]
  end

  it "retries only discarded members while retaining identity and history" do
    success = WorkflowMemberJob.new("success")
    discarded = WorkflowDiscardOnceJob.new("eventual success")
    batch = DecentJob::Batch.enqueue([success, discarded], database: database, on_finish: WorkflowCallbackJob)
    drain
    ids = batch.job_instances.map(&:provider_job_id)
    batch.retry
    _(batch.discarded?).must_equal false
    _(batch.jobs_finished?).must_equal false
    drain
    _(batch.reload.succeeded?).must_equal true
    _(batch.job_instances.map(&:provider_job_id)).must_equal ids
    _(DecentJob::Store.new(database).executions(success.provider_job_id).size).must_equal 1
    _(DecentJob::Store.new(database).executions(discarded.provider_job_id).size).must_equal 2
    _(callback_events.size).must_equal 2
  end

  it "fires discard before enqueue but defers finish until enqueue" do
    batch = DecentJob::Batch.new(database: database, on_discard: WorkflowCallbackJob, on_finish: WorkflowCallbackJob)
    batch.add(WorkflowDiscardOnceJob.new("failed"))
    drain
    _(batch.reload.enqueued?).must_equal false
    _(batch.jobs_finished?).must_equal false
    _(callback_events.map { |_, event, _| event }).must_equal [:discard]
    batch.enqueue
    drain
    _(callback_events.map { |_, event, _| event }).must_equal [:finish]
  end

  it "reopens the same batch recursively during inline callbacks" do
    inline = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    @classes.each { |klass| klass.queue_adapter = inline }
    batch = DecentJob::Batch.enqueue(database: database, on_finish: WorkflowRecursiveJob, stage: 1)
    _(batch.reload.finished?).must_equal true
    _(batch.properties).must_equal({stage: 3})
    _(batch.job_instances.size).must_equal 4
    _(batch.callback_job_instances.size).must_equal 3
    _(WorkflowCallbackJob.events.size).must_equal 3
  end

  it "adds the currently executing job and uses its new membership at completion" do
    parent = WorkflowAttachingJob.perform_later("parent")
    drain
    events = []
    events << WorkflowMemberJob.events.pop until WorkflowMemberJob.events.empty?
    created = events.find { |event| event.is_a?(Array) }
    _(created[1]).must_equal created[2]
    batch = DecentJob::Batch.find(created[1], database: database)
    _(batch.reload.succeeded?).must_equal true
    _(batch.job_instances.map(&:provider_job_id)).must_include parent.provider_job_id
    _(callback_events.size).must_equal 1
  end

  it "does not leak batch membership into inline children or an enclosing bulk capture" do
    inline = DecentJob::Adapter.new(database: database, execution_mode: :inline)
    @classes.each { |klass| klass.queue_adapter = inline }
    batch = nil

    outer = DecentJob::Bulk.enqueue do
      batch = DecentJob::Batch.enqueue([WorkflowChildJob.new], database: database)
      _(WorkflowMemberJob.events.size).must_equal 1
    end

    _(outer).must_be_empty
    _(batch.job_instances.size).must_equal 1

    database.with_connection do |connection|
      _(connection.exec("SELECT batch_id FROM decent_jobs WHERE job_class = 'WorkflowMemberJob'").getvalue(0, 0)).must_be_nil
    end
  end

  it "deduplicates callbacks when final members finish concurrently" do
    jobs = 2.times.map { WorkflowBlockingJob.new }
    batch = DecentJob::Batch.enqueue(jobs, database: database, on_finish: WorkflowCallbackJob, on_success: WorkflowCallbackJob)

    jobs.each do |job|
      @threads << Thread.new { @performer.perform_one(job_id: job.provider_job_id) }
    end

    2.times { Timeout.timeout(5) { WorkflowBlockingJob.started.pop } }
    2.times { WorkflowBlockingJob.release << true }
    @threads.each(&:value)
    _(batch.reload.jobs_finished?).must_equal true
    _(batch.callback_job_instances.size).must_equal 2
    3.times { DecentJob::BatchStore.new(database).reconcile(batch.id) }
    drain
    _(callback_events.size).must_equal 2
    _(batch.reload.succeeded?).must_equal true
  end

  it "does not emit success when a member fails during reconciliation" do
    job = WorkflowDiscardOnceJob.new("failed")
    batch = DecentJob::Batch.enqueue([job], database: database,
      on_success: WorkflowCallbackJob, on_discard: WorkflowCallbackJob, on_finish: WorkflowCallbackJob)
    complete_during_reconciliation(batch, job)
    _(batch.callback_job_instances.map { |callback| callback.arguments.last.fetch(:event) }.sort).must_equal [:discard, :finish]
    drain
    _(batch.reload.discarded?).must_equal true
    _(batch.finished?).must_equal true
  end

  it "waits for the discard callback when a callback fails during reconciliation" do
    batch = DecentJob::Batch.enqueue(database: database,
      on_finish: WorkflowFailOnceCallbackJob, on_discard: WorkflowCallbackJob)
    complete_during_reconciliation(batch, batch.callback_job_instances.first)
    _(batch.reload.discarded?).must_equal true
    _(batch.finished?).must_equal false
    _(batch.callback_job_instances.size).must_equal 2
    drain
    _(batch.reload.finished?).must_equal true
    _(callback_events.map { |_, event, _| event }).must_equal [:discard]
  end

  it "keeps admission-rejected callbacks pending and dispatches them when allowed" do
    WorkflowLimitedCallbackJob.decent_job_control_concurrency_with(total_limit: 0)
    batch = DecentJob::Batch.enqueue(database: database, on_finish: WorkflowLimitedCallbackJob)
    _(batch.reload.jobs_finished?).must_equal true
    _(batch.finished?).must_equal false
    _(batch.callback_job_instances).must_be_empty
    WorkflowLimitedCallbackJob.decent_job_control_concurrency_with(total_limit: 1)
    drain
    _(batch.reload.finished?).must_equal true
    _(batch.callback_job_instances.size).must_equal 1
    _(callback_events.size).must_equal 1
  end

  it "persists jobs created by a callback's enqueue callbacks" do
    batch = DecentJob::Batch.enqueue(database: database, on_finish: WorkflowEnqueuingCallbackJob)
    _(batch.callback_job_instances.size).must_equal 2
    drain
    _(WorkflowMemberJob.events.pop).must_equal "callback child"
    _(callback_events.size).must_equal 1
    _(batch.reload.finished?).must_equal true
  end

  it "prepares controlled bulk callback children exactly once before admission" do
    WorkflowControlledBulkChildJob.decent_job_concurrency_rule(label: "callback-child", total_limit: 1)
    batch = DecentJob::Batch.enqueue(database: database, on_finish: WorkflowBulkChildrenCallbackJob)
    children = batch.callback_job_instances.select { |job| job.is_a?(WorkflowControlledBulkChildJob) }

    _(WorkflowBulkChildrenCallbackJob.enqueue_events).must_equal [:before, :after]
    _(WorkflowControlledBulkChildJob.enqueue_events.map(&:first)).must_equal [:before, :after, :before, :after]
    _(WorkflowControlledBulkChildJob.enqueue_events.map(&:last).tally.values).must_equal [2, 2]
    _(children.size).must_equal 1
    _(children.first.queue_name).must_equal "callback-child"
    _(children.first.decent_job_labels).must_equal ["callback-child"]
    _(batch.callback_job_instances.size).must_equal 2
    drain
    _(WorkflowMemberJob.events.pop).must_equal "first child"
    _(callback_events.size).must_equal 1
    _(batch.reload.finished?).must_equal true
  end

  it "marks callback failure discarded and reopens without replaying old failed callbacks" do
    batch = DecentJob::Batch.enqueue(database: database, on_finish: WorkflowFailOnceCallbackJob, on_discard: WorkflowCallbackJob)
    drain
    _(batch.reload.discarded?).must_equal true
    _(batch.finished?).must_equal true
    old_jobs = batch.callback_job_instances
    batch.retry
    drain
    _(batch.reload.succeeded?).must_equal true
    _(batch.callback_job_instances.size).must_equal 3
    old_jobs.each { |job| _(DecentJob::Store.new(database).executions(job.provider_job_id).size).must_equal 1 }
  end

  it "does not redispatch an event when its callback job history is deleted" do
    batch = DecentJob::Batch.enqueue(database: database, on_finish: WorkflowCallbackJob)
    callback = batch.callback_job_instances.first
    database.with_connection { |connection| connection.exec_params("DELETE FROM decent_jobs WHERE id = $1", [callback.provider_job_id]) }
    DecentJob::BatchStore.new(database).reconcile(batch.id)
    _(batch.reload.finished?).must_equal true
    _(batch.callback_job_instances).must_be_empty
    _(drain).must_equal 0
  end

  it "serializes opposing callback scope orders without deadlocking" do
    gate = Queue.new
    orders = [{finish: "A", success: "B"}, {finish: "B", success: "A"}]

    @threads = orders.map do |keys|
      Thread.new do
        gate.pop
        DecentJob::Batch.enqueue(database: database, on_finish: WorkflowKeyCallbackJob, on_success: WorkflowKeyCallbackJob, keys: keys)
      end
    end

    2.times { gate << true }
    batches = Timeout.timeout(10) { @threads.map(&:value) }
    _(batches.map { |batch| batch.callback_job_instances.size }).must_equal [2, 2]
    drain
    _(batches.all? { |batch| batch.reload.succeeded? }).must_equal true
    _(callback_events.size).must_equal 4
  end

  it "finishes empty and populated batches with asynchronous workers" do
    DecentJob.reset!
    errors = Queue.new
    DecentJob.configuration.max_threads = 2
    DecentJob.configuration.poll_interval = 0.02
    DecentJob.configuration.on_thread_error = ->(error) { errors << error }
    async = DecentJob::Adapter.new(database: database, execution_mode: :async_all)
    @classes.each { |klass| klass.queue_adapter = async }
    batches = 2.times.map { DecentJob::Batch.new(database: database, on_finish: WorkflowCallbackJob) }
    DecentJob::Batch.enqueue_all([[batches.first, []], [batches.last, [WorkflowMemberJob.new("async")]]])
    Timeout.timeout(5) { sleep 0.01 until batches.all? { |batch| batch.reload.finished? } }
    _(batches.all?(&:succeeded?)).must_equal true
    _(callback_events.size).must_equal 2
    _(errors).must_be_empty
  ensure
    async&.shutdown(timeout: 5)
    DecentJob.reset!
  end
end
