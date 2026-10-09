# frozen_string_literal: true

require "minitest/mock"
require "open3"
require "spec_helper"
require "stringio"
require "tmpdir"
require "decent_job/cli"

describe "DecentJob CLI logging" do
  before do
    @destinations = DecentJob::LogSubscriber.loggers.dup
    @stdout = StringIO.new
    @stderr = StringIO.new
    @cli = DecentJob::CLI.new(stdout: @stdout, stderr: @stderr)
    DecentJob::LogSubscriber.loggers.clear
    DecentJob::LogSubscriber.reset_logger
    DecentJob::CLI.within_exe = nil
    DecentJob::CLI.log_to_stdout = nil
  end

  after do
    DecentJob::CLI.within_exe = nil
    DecentJob::CLI.log_to_stdout = nil
    DecentJob::LogSubscriber.loggers.replace(@destinations)
    DecentJob::LogSubscriber.reset_logger
    DecentJob.reset!
  end

  it "defaults stdout logging to executable use and respects explicit overrides" do
    _(DecentJob::CLI.within_exe?).must_be_nil
    _(DecentJob::CLI.log_to_stdout?).must_be_nil
    DecentJob::CLI.within_exe = true
    _(DecentJob::CLI.log_to_stdout?).must_equal true
    DecentJob::CLI.log_to_stdout = false
    _(DecentJob::CLI.log_to_stdout?).must_equal false
    DecentJob::CLI.within_exe = false
    DecentJob::CLI.log_to_stdout = true
    _(DecentJob::CLI.log_to_stdout?).must_equal true
  end

  it "adds one tagged stdout destination across repeated calls" do
    DecentJob::CLI.within_exe = true
    DecentJob.stub(:cleanup_preserved_jobs, ->(**) {
      ActiveSupport::Notifications.instrument("notifier_listen.decent_job")
      0
    }) do
      2.times { _(@cli.run(["cleanup"])).must_equal 0 }
    end
    _(DecentJob::LogSubscriber.loggers.size).must_equal 1
    _(@stdout.string.scan("[DecentJob] Notifier subscribed with LISTEN").size).must_equal 2
  end

  it "does not add stdout when logging is explicitly disabled" do
    DecentJob::CLI.within_exe = true
    DecentJob::CLI.log_to_stdout = false
    DecentJob.stub(:cleanup_preserved_jobs, 0) { _(@cli.run(["cleanup"])).must_equal 0 }
    _(DecentJob::LogSubscriber.loggers).must_equal []
  end

  it "recognizes stdout inside an existing broadcast destination" do
    DecentJob::CLI.log_to_stdout = true
    destination = ActiveSupport::BroadcastLogger.new(Logger.new(@stdout))
    DecentJob::LogSubscriber.loggers << destination
    DecentJob::LogSubscriber.reset_logger
    DecentJob.stub(:cleanup_preserved_jobs, 0) { _(@cli.run(["cleanup"])).must_equal 0 }
    _(DecentJob::LogSubscriber.loggers).must_equal [destination]
  end

  it "recognizes stdout inside nested broadcasts without altering destinations or levels" do
    DecentJob::CLI.log_to_stdout = true
    stdout_logger = Logger.new(@stdout, level: Logger::INFO)
    error_output = StringIO.new
    error_logger = Logger.new(error_output, level: Logger::ERROR)
    inner = ActiveSupport::BroadcastLogger.new(error_logger, stdout_logger)
    destination = ActiveSupport::BroadcastLogger.new(inner)
    DecentJob::LogSubscriber.loggers << destination
    DecentJob::LogSubscriber.reset_logger
    DecentJob.stub(:cleanup_preserved_jobs, ->(**) {
      ActiveSupport::Notifications.instrument("notifier_listen.decent_job")
      0
    }) do
      2.times { _(@cli.run(["cleanup"])).must_equal 0 }
    end
    _(DecentJob::LogSubscriber.loggers).must_equal [destination]
    _(destination.broadcasts).must_equal [inner]
    _(inner.broadcasts).must_equal [error_logger, stdout_logger]
    _(@stdout.string.scan("Notifier subscribed with LISTEN").size).must_equal 2
    _(error_output.string).must_equal ""
    _(error_logger.level).must_equal Logger::ERROR
    _(stdout_logger.level).must_equal Logger::INFO
  end

  it "honors application stdout preferences after requiring the application" do
    Dir.mktmpdir do |directory|
      path = File.join(directory, "app.rb")
      File.write(path, "DecentJob::CLI.log_to_stdout = false\n")
      DecentJob::CLI.within_exe = true
      DecentJob.stub(:cleanup_preserved_jobs, 0) { _(@cli.run(["cleanup", "--require", path])).must_equal 0 }
      _(DecentJob::LogSubscriber.loggers).must_equal []
    end
  end

  it "marks executable context before requiring the application" do
    Dir.mktmpdir do |directory|
      path = File.join(directory, "app.rb")
      File.write(path, <<~RUBY)
        raise "not executable context" unless DecentJob::CLI.within_exe?
        DecentJob::CLI.log_to_stdout = false
        def DecentJob.cleanup_preserved_jobs(**) = 0
      RUBY
      output, status = Open3.capture2e(RbConfig.ruby, "-Ilib", "exe/decent_job", "cleanup", "--require", path)
      _(status.success?).must_equal true, output
      _(output).must_include "Deleted 0"
    end
  end
end

describe "DecentJob default logging" do
  it "follows configuration, direct assignment and reset after the broadcast is cached" do
    script = <<~RUBY
      require "decent_job"
      require "stringio"
      subscriber = DecentJob::LogSubscriber
      raise "not tagged" unless DecentJob.logger.respond_to?(:tagged)
      original = subscriber.logger
      first_output = StringIO.new
      first = Logger.new(first_output)
      DecentJob.configure { |config| config.logger = first }
      raise "stale configuration" unless subscriber.loggers == [first]
      raise "stale broadcast" if subscriber.logger.equal?(original)
      ActiveSupport::Notifications.instrument("notifier_listen.decent_job")
      raise "missing configured message" unless first_output.string.include?("LISTEN")
      second_output = StringIO.new
      second = Logger.new(second_output)
      DecentJob.logger = second
      raise "setter not shared" unless DecentJob.configuration.logger.equal?(second)
      ActiveSupport::Notifications.instrument("notifier_unlisten.decent_job")
      raise "missing assigned message" unless second_output.string.include?("UNLISTEN")
      raise "stale sink" if first_output.string.include?("UNLISTEN")
      DecentJob.reset!
      raise "stale reset" unless subscriber.loggers == [DecentJob.logger]
      raise "not reset" if DecentJob.logger.equal?(second)
      raise "cached reset" unless subscriber.logger.broadcasts == [DecentJob.logger]
      raise "Rails loaded" if defined?(Rails)
      raise "AR loaded" if defined?(ActiveRecord)
    RUBY
    output, status = Open3.capture2e(RbConfig.ruby, "-Ilib", "-e", script)
    _(status.success?).must_equal true, output
  end

  it "keeps appended destinations when the runtime changes before logger is accessed" do
    script = <<~RUBY
      require "decent_job"
      require "stringio"
      subscriber = DecentJob::LogSubscriber
      initial = subscriber.logger
      first = DecentJob.logger
      extra = Logger.new(StringIO.new)
      subscriber.loggers << extra
      DecentJob.reset!
      raise "explicit list lost" unless subscriber.loggers == [first, extra]
      subscriber.reset_logger
      raise "broadcast wrong" unless subscriber.logger.broadcasts == [first, extra]
    RUBY
    output, status = Open3.capture2e(RbConfig.ruby, "-Ilib", "-e", script)
    _(status.success?).must_equal true, output
  end
end
