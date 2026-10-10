# frozen_string_literal: true

require "rubocop"

%w[statement_spacing empty_lines_around_multiline_statement empty_line_before_return empty_line_after_guard_clause].each do |name|
  require File.expand_path("../../rubocop/cop/decent_job/#{name}", __dir__)
end

# Runs one cop over a source string and returns its offenses and autocorrected output.
module CopRunner
  Result = Struct.new(:offenses, :corrected)

  def run_cop(cop_class, source)
    cop = cop_class.new(RuboCop::Config.new({}, ""), {autocorrect: true})
    processed_source = RuboCop::ProcessedSource.new(source, 3.2, "example.rb")
    report = RuboCop::Cop::Commissioner.new([cop], [], raise_error: true).investigate(processed_source)
    corrector = report.correctors.compact.first
    Result.new(report.offenses.map { |offense| [offense.line, offense.message] }, corrector ? corrector.rewrite : source)
  end
end
