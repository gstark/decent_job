# frozen_string_literal: true

require "spec_helper"

describe "cleanup tracker" do
  it "defaults to disabled and exposes resettable counters" do
    tracker = DecentJob::CleanupTracker.new
    _(tracker.cleanup_interval_jobs).must_equal false
    _(tracker.cleanup_interval_seconds).must_equal false
    tracker.increment
    _(tracker.job_count).must_equal 1
    _(tracker.cleanup?).must_equal false
    tracker.reset
    _(tracker.job_count).must_equal 0
    _(tracker.last_at).must_be_kind_of Time
  end

  it "uses strict job and time boundaries" do
    tracker = DecentJob::CleanupTracker.new(cleanup_interval_jobs: 2, cleanup_interval_seconds: 60)
    tracker.last_at = Time.now + 3600
    2.times { tracker.increment }
    _(tracker.cleanup?).must_equal false
    tracker.increment
    _(tracker.cleanup?).must_equal true
    tracker.reset
    tracker.last_at = Time.now - 61
    _(tracker.cleanup?).must_equal true
    tracker.cleanup_interval_seconds = false
    _(tracker.cleanup?).must_equal false
  end

  it "accepts always and rejects invalid intervals" do
    %i[cleanup_interval_jobs cleanup_interval_seconds].each do |name|
      _(DecentJob::CleanupTracker.new(**{name => -1}).cleanup?).must_equal true
      [0, nil, true, "2", -2, Float::NAN, Float::INFINITY, Complex(1, 1)].each do |value|
        _ { DecentJob::CleanupTracker.new(**{name => value}) }.must_raise ArgumentError
      end
    end
  end
end

describe "cleanup configuration" do
  it "defaults to fourteen days and periodic cleanup including discarded jobs" do
    config = DecentJob::Configuration.new
    _(config.cleanup_preserved_jobs_before_seconds_ago).must_equal 1_209_600
    _(config.cleanup_discarded_jobs?).must_equal true
    _(config.cleanup_interval_jobs).must_equal 1000
    _(config.cleanup_interval_seconds).must_equal 600
    _(config.worker_options.slice(:cleanup_interval_jobs, :cleanup_interval_seconds)).must_equal({cleanup_interval_jobs: 1000, cleanup_interval_seconds: 600})
  end

  it "normalizes interval settings strictly" do
    config = DecentJob::Configuration.new
    {nil => 1000, "" => 1000, true => 1000, false => false, "false" => false, 0 => false, "0" => false, -1 => -1, "-1" => -1, "12" => 12}.each do |value, expected|
      config.cleanup_interval_jobs = value
      _(config.cleanup_interval_jobs).must_equal expected
    end
    ["invalid", -2, Float::NAN, Float::INFINITY, 0.5].each do |value|
      _ { config.cleanup_interval_jobs = value }.must_raise ArgumentError
    end
    config.cleanup_interval_seconds = 0.5
    _(config.cleanup_interval_seconds).must_equal 0.5
  end

  it "reads environment settings and allows application overrides" do
    settings = {"DECENT_JOB_CLEANUP_INTERVAL_JOBS" => "23", "DECENT_JOB_CLEANUP_INTERVAL_SECONDS" => "false", "DECENT_JOB_CLEANUP_DISCARDED_JOBS" => "false", "DECENT_JOB_CLEANUP_PRESERVED_JOBS_BEFORE_SECONDS_AGO" => "7200"}
    previous = ENV.to_h.slice(*settings.keys)
    begin
      ENV.update(settings)
      config = DecentJob::Configuration.new
      _(config.cleanup_interval_jobs).must_equal 23
      _(config.cleanup_interval_seconds).must_equal false
      _(config.cleanup_discarded_jobs?).must_equal false
      _(config.cleanup_preserved_jobs_before_seconds_ago).must_equal 7200
      config.cleanup_interval_jobs = 42
      config.cleanup_discarded_jobs = true
      _(config.cleanup_interval_jobs).must_equal 42
      _(config.cleanup_discarded_jobs?).must_equal true
    ensure
      settings.each_key { |key| ENV.delete(key) }
      ENV.update(previous)
    end
  end
end
