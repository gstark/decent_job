# frozen_string_literal: true

require "spec_helper"
require "support/cop_runner"

describe RuboCop::Cop::DecentJob::EmptyLinesAroundMultilineStatement do
  include CopRunner

  def inspect_source(source)
    run_cop(RuboCop::Cop::DecentJob::EmptyLinesAroundMultilineStatement, source)
  end

  it "surrounds a multi-line conditional with blank lines" do
    result = inspect_source(<<~RUBY)
      def validate
        timeout = configuration.timeout
        unless timeout.positive?
          errors.add(:timeout)
        end
        validate_queues
      end
    RUBY
    _(result.offenses).must_equal [
      [3, "Add an empty line before this multi-line statement."],
      [5, "Add an empty line after this multi-line statement."]
    ]
    _(result.corrected).must_equal <<~RUBY
      def validate
        timeout = configuration.timeout

        unless timeout.positive?
          errors.add(:timeout)
        end

        validate_queues
      end
    RUBY
  end

  it "treats an assignment that ends with a block as a multi-line statement" do
    result = inspect_source(<<~RUBY)
      def validate
        values = configuration.values
        valid = values.is_a?(Array) && values.all? do |value|
          value.is_a?(Class)
        end
        errors.add(:values) unless valid
      end
    RUBY
    _(result.corrected).must_equal <<~RUBY
      def validate
        values = configuration.values

        valid = values.is_a?(Array) && values.all? do |value|
          value.is_a?(Class)
        end

        errors.add(:values) unless valid
      end
    RUBY
  end

  it "ignores single-line statements, wrapped method calls, wrapped modifiers, and wrapped ternaries" do
    source = <<~RUBY
      def configure
        return if ready?
        raise ArgumentError, "bad port" unless port.is_a?(Integer)
        @handler = klass.new(app, port: port,
          request_timeout: request_timeout)
        return false unless prepare(handles,
          deadline: deadline)
        status = Subprocess.new(configuration: recipe,
          index: index).run ? 0 : 1
        @handler.start(status)
      end
    RUBY
    _(inspect_source(source).offenses).must_equal []
  end

  it "leaves the first and last statements of a body alone" do
    source = <<~RUBY
      def validate
        unless cron.is_a?(Hash)
          errors.add(:cron)
          return
        end

        cron.each do |key, attributes|
          check(key, attributes)
        rescue => error
          errors.add(:cron, error.message)
        end
      end
    RUBY
    _(inspect_source(source).offenses).must_equal []
  end

  it "keeps a comment attached to the statement below it" do
    result = inspect_source(<<~RUBY)
      def claim
        key = candidate.fetch("lock_key")
        # A session lock is reentrant.
        Thread.handle_interrupt(Object => :on_blocking) do
          lock(key)
        end
      end
    RUBY
    _(result.corrected).must_equal <<~RUBY
      def claim
        key = candidate.fetch("lock_key")

        # A session lock is reentrant.
        Thread.handle_interrupt(Object => :on_blocking) do
          lock(key)
        end
      end
    RUBY
  end

  it "inserts the blank line after a heredoc terminator" do
    result = inspect_source(<<~RUBY)
      def scope
        sql = <<~SQL
          SELECT 1
        SQL
        database.with_connection do |connection|
          connection.exec(sql)
        end
      end
    RUBY
    _(result.corrected).must_equal <<~RUBY
      def scope
        sql = <<~SQL
          SELECT 1
        SQL

        database.with_connection do |connection|
          connection.exec(sql)
        end
      end
    RUBY
  end

  it "separates consecutive blocks with one blank line" do
    result = inspect_source(<<~RUBY)
      def teardown
        begin
          manager.shutdown
        rescue ShutdownFailure
          nil
        end
        begin
          reset!
        rescue ShutdownFailure
          nil
        end
      end
    RUBY
    _(result.offenses.size).must_equal 1
    _(result.corrected).must_equal <<~RUBY
      def teardown
        begin
          manager.shutdown
        rescue ShutdownFailure
          nil
        end

        begin
          reset!
        rescue ShutdownFailure
          nil
        end
      end
    RUBY
  end
end
