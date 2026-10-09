# frozen_string_literal: true

require "decent_job/cli"
require "minitest/mock"
require "spec_helper"
require "stringio"

describe "health service CLI lifecycle" do
  before { DecentJob.reset! }

  after do
    DecentJob.reset!
  end

  it "accepts probe flags and preserves startup failure when cleanup also fails" do
    output = StringIO.new
    error = StringIO.new
    cli = DecentJob::CLI.new(stdout: output, stderr: error)
    DecentJob.stub(:start, -> { raise "original startup failure" }) do
      DecentJob.stub(:shutdown, ->(**) { raise "secondary shutdown failure" }) do
        _(cli.run(["--probe-port", "0", "--probe-handler", "simple"])).must_equal 1
      end
    end
    _(DecentJob.configuration.probe_port).must_equal 0, error.string
    _(DecentJob.configuration.probe_handler).must_equal :simple
    _(error.string).must_include "original startup failure"
    _(error.string).wont_include "secondary shutdown failure"
  end

  it "rejects invalid probe ports before boot" do
    cli = DecentJob::CLI.new(stdout: StringIO.new, stderr: StringIO.new)
    _(cli.run(["--probe-port", "-1"])).must_equal 2
    _(cli.run(["--probe-port", "65536"])).must_equal 2
    _(cli.run(["--probe-port", "no"])).must_equal 2
  end
end
