# frozen_string_literal: true

require "spec_helper"
require "fileutils"
require "minitest/mock"
require "tmpdir"

describe "daemon fork resource safety" do
  include DatabaseSpec

  it "refuses borrowed database sessions before detaching and preserves their usability" do
    Dir.mktmpdir("decent-job-daemon") do |directory|
      path = File.join(directory, "worker.pid")
      instance = DecentJob::Daemon.new(pidfile: path)
      database.with_connection do |connection|
        Process.stub(:daemon, ->(*) { flunk "must refuse borrowed sessions before detaching" }) do
          error = _ { instance.daemonize }.must_raise DecentJob::ForkInProgress
          _(error.message).must_include "borrowed"
        end
        _(connection.exec("SELECT 1").getvalue(0, 0)).must_equal "1"
      end
      File.open(path, File::RDWR) do |file|
        _(file.flock(File::LOCK_EX | File::LOCK_NB)).must_equal 0
      end
    ensure
      instance&.cleanup
    end
  end
end
