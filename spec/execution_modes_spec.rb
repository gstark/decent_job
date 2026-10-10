# frozen_string_literal: true

require "spec_helper"

describe "execution modes" do
  before { DecentJob.reset! }
  after { DecentJob.reset! }

  # Evaluates in_webserver? from a frame that reports the given file and line.
  def in_webserver_from(frame)
    file, line = frame.split(":")
    DecentJob::Configuration.new.instance_eval("in_webserver?", file, line.to_i) # standard:disable Style/EvalWithLocation
  end

  it "keeps standalone defaults external and follows configuration changes" do
    adapter = DecentJob::Adapter.new
    _(adapter.execution_mode).must_equal :external
    DecentJob.configure { |config| config.execution_mode = :inline }
    _(adapter.execution_mode).must_equal :inline
    _(adapter.execute_inline?).must_equal true
  end

  it "rejects false as an explicitly supplied adapter execution mode" do
    _ { DecentJob::Adapter.new(execution_mode: false) }.must_raise ArgumentError
  end

  it "rejects false configuration modes while nil keeps the default" do
    config = DecentJob::Configuration.new
    config.execution_mode = false
    _ { config.validate! }.must_raise ArgumentError
    config.execution_mode = nil
    _(config.validate!.execution_mode).must_equal :external
  end

  it "preserves an explicit adapter override" do
    adapter = DecentJob::Adapter.new(execution_mode: :external)
    DecentJob.configure { |config| config.execution_mode = :inline }
    _(adapter.execution_mode).must_equal :external
    _(adapter.execute_external?).must_equal true
  end

  %i[async async_server].each do |mode|
    it "runs #{mode} externally outside webservers" do
      adapter = DecentJob::Adapter.new(execution_mode: mode)
      _(adapter.execute_async?).must_equal false
      _(adapter.execute_external?).must_equal true
      _(adapter.start_async).must_be_nil
    end

    it "runs #{mode} asynchronously inside webservers" do
      adapter = DecentJob::Adapter.new(execution_mode: mode)

      DecentJob.configuration.stub(:in_webserver?, true) do
        _(adapter.execute_async?).must_equal true
        _(adapter.execute_external?).must_equal false
      end
    end
  end

  it "allows async_all in every process" do
    DecentJob.configure { |config| config.execution_mode = :async_all }
    adapter = DecentJob::Adapter.new
    _(adapter.execute_async?).must_equal true
    _(adapter.execute_external?).must_equal false
    _(adapter.execute_inline?).must_equal false
  end

  ["config.ru:3", "/gems/puma/request.rb:76", "/gems/puma/response.rb:78",
    "/gems/puma/cluster/worker.rb:58", "/gems/puma/single.rb:44", "/gems/rack/handler/iodine.rb:13"].each do |frame|
    it "recognizes the server stack #{frame}" do
      _(in_webserver_from(frame)).must_equal true
    end
  end

  ["/gems/puma/cluster.rb:58", "/gems/rails/commands/console/console_command.rb:10", "db/migrate.rb:1"].each do |frame|
    it "does not treat #{frame} as a webserver" do
      _(in_webserver_from(frame)).must_equal false
    end
  end

  it "recognizes Rails::Server" do
    host = Module.new
    host.const_set(:Server, Class.new)
    Object.const_set(:Rails, host)
    _(DecentJob.configuration.in_webserver?).must_equal true
  ensure
    Object.send(:remove_const, :Rails)
  end

  %i[async async_all async_server].each do |mode|
    it "makes inherited #{mode} adapters external in controlled children" do
      adapter = DecentJob::Adapter.new(execution_mode: mode)
      adapter.reset_after_fork
      _(adapter.execution_mode).must_equal :external
    end
  end
end
