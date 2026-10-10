# frozen_string_literal: true

require "spec_helper"
require "support/cop_runner"

describe RuboCop::Cop::DecentJob::EmptyLineAfterGuardClause do
  include CopRunner

  def inspect_source(source)
    run_cop(RuboCop::Cop::DecentJob::EmptyLineAfterGuardClause, source)
  end

  it "adds a blank line after a modifier guard" do
    result = inspect_source(<<~RUBY)
      def take(pattern)
        match = pattern.match(@source, @position)
        return unless match
        @position = match.end(0)
      end
    RUBY
    _(result.offenses).must_equal [[3, "Add empty line after guard clause."]]
    _(result.corrected).must_equal <<~RUBY
      def take(pattern)
        match = pattern.match(@source, @position)
        return unless match

        @position = match.end(0)
      end
    RUBY
  end

  it "covers raise, break, and next guards inside blocks and loops" do
    result = inspect_source(<<~RUBY)
      def expand
        raise ArgumentError, "empty" if parts.empty?
        current = first
        (2 * @max + 1).times do
          expanded << current
          break if current == last
          current += 1
        end
        parts.each do |part|
          next if part.empty?
          result << part
        end
      end
    RUBY
    _(result.offenses.map(&:first)).must_equal [2, 6, 10]
  end

  it "leaves consecutive guards and a guard before end or else alone" do
    source = <<~RUBY
      def values
        return [] if parts.empty?
        return parts.first if parts.one?

        if parts.include?(nil)
          return nil if strict
        else
          raise ArgumentError, "mixed" unless parts.all?(Integer)
        end
      end
    RUBY
    _(inspect_source(source).offenses).must_equal []
  end
end
