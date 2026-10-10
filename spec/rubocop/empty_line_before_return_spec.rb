# frozen_string_literal: true

require "spec_helper"
require "support/cop_runner"

describe RuboCop::Cop::DecentJob::EmptyLineBeforeReturn do
  include CopRunner

  def inspect_source(source)
    run_cop(RuboCop::Cop::DecentJob::EmptyLineBeforeReturn, source)
  end

  it "adds a blank line before a return that follows another statement" do
    result = inspect_source(<<~RUBY)
      def foo
        x = 1 + 2
        return x
      end
    RUBY
    _(result.offenses).must_equal [[3, "Add an empty line before `return`."]]
    _(result.corrected).must_equal <<~RUBY
      def foo
        x = 1 + 2

        return x
      end
    RUBY
  end

  it "leaves a lone return alone" do
    source = <<~RUBY
      def bar
        return 1 + 2
      end
    RUBY
    _(inspect_source(source).offenses).must_equal []
  end

  it "leaves a return directly inside a conditional or loop body alone" do
    source = <<~RUBY
      def validate
        unless cron.is_a?(Hash)
          errors.add(:cron)
          return
        end
        case command
        when "help"
          puts parser
          return 0
        else
          log(command)
          return 1
        end
        while running?
          step
          return if done?
        end
      end
    RUBY
    _(inspect_source(source).offenses).must_equal []
  end

  it "ignores a modifier return and an existing blank line" do
    source = <<~RUBY
      def claim
        candidate = candidates.first
        return nil if candidate.nil?
        row = start(candidate)

        return row
      end
    RUBY
    _(inspect_source(source).offenses).must_equal []
  end

  it "applies inside block bodies and method bodies with a rescue clause" do
    result = inspect_source(<<~RUBY)
      def perform
        transaction do |connection|
          connection.exec("INSERT INTO examples VALUES (1)")
          return nil
        end
        schedule(id)
        return
      rescue => error
        log(error)
      end
    RUBY
    _(result.offenses.map(&:first).sort).must_equal [4, 7]
    _(result.corrected).must_equal <<~RUBY
      def perform
        transaction do |connection|
          connection.exec("INSERT INTO examples VALUES (1)")

          return nil
        end
        schedule(id)

        return
      rescue => error
        log(error)
      end
    RUBY
  end
end
