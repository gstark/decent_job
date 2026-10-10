# frozen_string_literal: true

require "spec_helper"
require "open3"
require "rbconfig"
require "stringio"

describe "DecentJob support layer" do
  it "notifies, isolates state and logs without ActiveSupport" do
    script = <<~RUBY
      require "json"
      require "time"
      require "decent_job/support"
      require "decent_job/isolated_state"
      require "decent_job/notifications"
      require "decent_job/logging"
      require "stringio"
      raise "ActiveSupport loaded" if defined?(ActiveSupport)

      events = []
      subscription = DecentJob::Notifications.subscribe(/\\.decent_job\\z/) { |event| events << event }
      value = DecentJob::Notifications.instrument("probe.decent_job", id: 1) { |payload| payload[:value] = 7 }
      raise "wrong value" unless value == 7
      raise "wrong event" unless events.map(&:name) == ["probe.decent_job"] && events.first.payload == {id: 1, value: 7}
      DecentJob::Notifications.unsubscribe(subscription)
      DecentJob::Notifications.instrument("probe.decent_job")
      raise "still subscribed" unless events.size == 1

      DecentJob::IsolatedState[:probe] = :main
      other = Thread.new { DecentJob::IsolatedState[:probe] }.value
      raise "state leaked" unless other.nil? && DecentJob::IsolatedState[:probe] == :main
      raise "wrong context" unless DecentJob::IsolatedState.context.equal?(Thread.current)

      output = StringIO.new
      logger = DecentJob::Logging.tagged_logger(output)
      logger.tagged("DecentJob", "42") { logger.info("hello") }
      raise "wrong log: \#{output.string.inspect}" unless output.string == "[DecentJob] [42] hello\\n"
      raise "tags kept" unless logger.formatter.current_tags.empty?
      raise "output lost" unless DecentJob::Logging.outputs_to?(DecentJob::Logging::BroadcastLogger.new(logger), output)

      raise "blank" unless DecentJob::Support.blank?("\\u00a0")
      raise "constantize" unless DecentJob::Support.constantize("::DecentJob::Logging").equal?(DecentJob::Logging)
    RUBY
    output, status = Open3.capture2e(RbConfig.ruby, "-Ilib", "-e", script)
    _(status.success?).must_equal true, output
  end

  it "runs every subscriber before raising their errors" do
    called = []
    first = DecentJob::Notifications.subscribe("broken.decent_job") { raise ArgumentError, "first" }
    second = DecentJob::Notifications.subscribe("broken.decent_job") { called << :second }
    error = _ { DecentJob::Notifications.instrument("broken.decent_job") }.must_raise ArgumentError
    _(error.message).must_equal "first"
    _(called).must_equal [:second]

    third = DecentJob::Notifications.subscribe("broken.decent_job") { raise "third" }
    error = _ { DecentJob::Notifications.instrument("broken.decent_job") }.must_raise DecentJob::Notifications::SubscriberError
    _(error.exceptions.map(&:message)).must_equal ["first", "third"]
  ensure
    [first, second, third].compact.each { |subscription| DecentJob::Notifications.unsubscribe(subscription) }
  end

  it "keeps the block exception when a subscriber also raises" do
    subscription = DecentJob::Notifications.subscribe("both.decent_job") { raise ArgumentError, "subscriber" }
    error = _ { DecentJob::Notifications.instrument("both.decent_job") { raise KeyError, "block" } }.must_raise KeyError
    _(error.message).must_equal "block"
  ensure
    DecentJob::Notifications.unsubscribe(subscription) if subscription
  end

  it "forwards events and block exceptions to ActiveSupport subscribers" do
    events = []
    subscription = ActiveSupport::Notifications.subscribe("forward.decent_job") { |event| events << event }
    _ { DecentJob::Notifications.instrument("forward.decent_job", id: 3) { raise "boom" } }.must_raise RuntimeError
    _(events.size).must_equal 1
    _(events.first.payload[:id]).must_equal 3
    _(events.first.payload[:exception]).must_equal ["RuntimeError", "boom"]
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription)
  end

  it "scopes logger tags per thread" do
    output = StringIO.new
    logger = DecentJob::Logging.tagged_logger(output)
    logger.tagged("outer") do
      Thread.new { logger.info("other") }.join
      logger.info("inner")
    end
    _(output.string.lines).must_equal ["other\n", "[outer] inner\n"]
  end

  it "compares blank values like ActiveSupport" do
    [nil, false, "", "  ", "\u00a0", [], {}].each { |value| _(DecentJob::Support.blank?(value)).must_equal true }
    ["a", [1], {a: 1}, 0, true].each { |value| _(DecentJob::Support.present?(value)).must_equal true }
  end
end
