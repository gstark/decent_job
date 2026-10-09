# frozen_string_literal: true

require "fileutils"
require "open3"
require "rubygems/package"
require "spec_helper"
require "tmpdir"

describe "standalone gem packaging" do
  # GoodJob ad4be5c2 loads Rails/Active Record in its engine. DecentJob's
  # dependency and standalone-boot adaptation needs its own executable proof.
  it "ships its SQL and executable and boots from an unpacked gem without Rails" do
    Dir.mktmpdir("decent-job-package-") do |directory|
      root = File.expand_path("..", __dir__)
      artifact = File.join(directory, "decent_job.gem")
      output, status = Open3.capture2e(RbConfig.ruby, "-S", "gem", "build", "decent_job.gemspec",
        "--output", artifact, chdir: root)
      _(status.success?).must_equal true, output

      package = Gem::Package.new(artifact)
      _(package.spec.runtime_dependencies.map(&:name).sort).must_equal %w[activejob pg]
      _(package.spec.executables).must_equal ["decent_job"]
      unpacked = File.join(directory, "unpacked")
      package.extract_files(unpacked)
      _(Dir[File.join(unpacked, "db/migrations/*.sql")].map { |path| File.basename(path) }.sort)
        .must_equal Dir[File.join(root, "db/migrations/*.sql")].map { |path| File.basename(path) }.sort
      _(File.file?(File.join(unpacked, "exe/decent_job"))).must_equal true

      environment = {"RUBYOPT" => nil, "RUBYLIB" => nil, "BUNDLE_GEMFILE" => nil,
                     "DECENT_JOB_DATABASE_URL" => nil, "DATABASE_URL" => nil, "PATH" => directory}
      program = <<~RUBY
        gem "activejob", #{Gem.loaded_specs.fetch("activejob").version.to_s.inspect}
        gem "pg", #{Gem.loaded_specs.fetch("pg").version.to_s.inspect}
        require "decent_job"
        puts JSON.generate(
          loaded_from: $LOADED_FEATURES.find { |path| path.end_with?("/lib/decent_job.rb") },
          adapter: ActiveJob::QueueAdapters.lookup(:decent_job).name,
          shutdown: DecentJob.shutdown?,
          zero_attempts: DecentJob.perform_inline(limit: 0),
          forbidden_dependencies: Gem.loaded_specs.keys & %w[rails railties activerecord activemodel fugit thor rack],
          frameworks: [defined?(Rails), defined?(ActiveRecord), defined?(ActiveModel)].compact
        )
      RUBY
      output, error, status = Open3.capture3(environment, RbConfig.ruby, "-I#{unpacked}/lib", "-e", program, chdir: directory)
      _(status.success?).must_equal true, error
      result = JSON.parse(output)
      _(File.realpath(result.fetch("loaded_from"))).must_equal File.realpath(File.join(unpacked, "lib/decent_job.rb"))
      _(result.fetch("adapter")).must_equal "ActiveJob::QueueAdapters::DecentJobAdapter"
      _(result.fetch("shutdown")).must_equal true
      _(result.fetch("zero_attempts")).must_equal 0
      _(result.fetch("forbidden_dependencies")).must_be_empty
      _(result.fetch("frameworks")).must_be_empty

      output, status = Open3.capture2e(environment, RbConfig.ruby, "-I#{unpacked}/lib",
        File.join(unpacked, "exe/decent_job"), "--help", chdir: directory)
      _(status.success?).must_equal true, output
      _(output).must_include "Usage:"
    end
  end
end
