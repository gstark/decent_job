# frozen_string_literal: true

require "spec_helper"
require "decent_job/queue_selection"

describe DecentJob::QueueSelection do
  it "selects every queue by default" do
    selection = DecentJob::QueueSelection.new
    _(selection.queues).must_equal []
    _(selection.exclude?).must_equal false
    _(selection.ordered?).must_equal false
    _(selection.include?("default")).must_equal true
    _(selection.include?("other")).must_equal true
  end

  it "selects listed queues and strips surrounding whitespace" do
    selection = DecentJob::QueueSelection.new(" default, mailers ")
    _(selection.queues).must_equal ["default", "mailers"]
    _(selection.exclude?).must_equal false
    _(selection.ordered?).must_equal false
    _(selection.include?("mailers")).must_equal true
    _(selection.include?("other")).must_equal false
  end

  it "excludes every queue named after the exclusion modifier" do
    selection = DecentJob::QueueSelection.new(" - default, mailers ")
    _(selection.queues).must_equal ["default", "mailers"]
    _(selection.exclude?).must_equal true
    _(selection.ordered?).must_equal false
    _(selection.include?("default")).must_equal false
    _(selection.include?("mailers")).must_equal false
    _(selection.include?("other")).must_equal true
  end

  it "preserves queue order when the order modifier is supplied" do
    selection = DecentJob::QueueSelection.new("+mailers,default")
    _(selection.queues).must_equal ["mailers", "default"]
    _(selection.exclude?).must_equal false
    _(selection.ordered?).must_equal true
    _(selection.include?("default")).must_equal true
    _(selection.include?("other")).must_equal false
  end

  it "permits ordinary identifier characters" do
    selection = DecentJob::QueueSelection.new("tenant_1.email,urgent/jobs,high-priority,foo-bar")
    _(selection.queues).must_equal ["tenant_1.email", "urgent/jobs", "high-priority", "foo-bar"]
  end

  it "permits internal modifiers in queue names after a leading modifier" do
    selection = DecentJob::QueueSelection.new("+high-priority,foo+bar")
    _(selection.queues).must_equal ["high-priority", "foo+bar"]
    _(selection.ordered?).must_equal true
  end

  it "rejects empty entries, reserved syntax, and combined modifiers" do
    ["", " ", ",", "a,", ",a", "a,,b", "a, ,b", "-", "+", "+-a", "-+a", "--a", "++a", "a,-b", "a,+b", "a*b", "*,a", "a,*", "+*", "-*", "a:2", "a;b"].each do |expression|
      _ { DecentJob::QueueSelection.new(expression) }.must_raise ArgumentError
    end
  end

  it "parses independent pool selections and thread counts" do
    pools = DecentJob::QueueSelection.pools("a:2; -a:3; *", default_threads: 5)
    _(pools.map { |pool| pool[:threads] }).must_equal [2, 3, 5]
    _(pools.map { |pool| pool[:selection].queues }).must_equal [["a"], ["a"], []]
    _(pools[1][:selection].exclude?).must_equal true
    _(pools[2][:selection].include?("anything")).must_equal true
  end

  it "accepts whitespace around pool thread counts" do
    pool = DecentJob::QueueSelection.pools(" +a,b : 2 ", default_threads: 1).first
    _(pool[:selection].ordered?).must_equal true
    _(pool[:threads]).must_equal 2
  end

  it "rejects empty pools and malformed thread counts" do
    ["", " ", ";", "a;", ";a", "a;;b", "a; ;b", "a:", "a:0", "a:-1", "a:1.5", "a:two", "a:2:3", "a:+2", ":2"].each do |expression|
      _ { DecentJob::QueueSelection.pools(expression, default_threads: 3) }.must_raise ArgumentError
    end
  end

  it "requires positive integer default thread counts" do
    [0, -1, 1.5, "2", nil].each do |threads|
      _ { DecentJob::QueueSelection.pools("*", default_threads: threads) }.must_raise ArgumentError
    end
  end
end
