# frozen_string_literal: true

require "fileutils"
require "pg"
require "securerandom"
require "timeout"
require "tmpdir"

# Every fixture owns its database, Unix socket, credentials and foreground process.
# Session search_path cannot isolate tests in transaction pooling mode.
class PgBouncerFixture
  attr_reader :connection_options, :directory

  def initialize
    executable = ENV.fetch("PGBOUNCER_BIN", "pgbouncer")
    candidates = executable.include?("/") ? [executable] : ENV.fetch("PATH").split(File::PATH_SEPARATOR).map { |path| File.join(path, executable) }
    @executable = candidates.find { |path| File.file?(path) && File.executable?(path) }
    raise "PgBouncer integration requires pgbouncer on PATH (or PGBOUNCER_BIN)" unless @executable

    @directory = Dir.mktmpdir("dj-pgb-", "/tmp")
    # Minitest interrupts can bypass example teardown; process exit must still
    # remove the database and stop this fixture's foreground proxy.
    at_exit { close }
    @admin = PG.connect(ENV.fetch("DECENT_JOB_TEST_DATABASE_URL", "postgres:///postgres"))
    @database_name = "decent_job_pgb_#{SecureRandom.hex(10)}"
    @admin.exec("CREATE DATABASE #{@admin.quote_ident(@database_name)}")
    @database_created = true
    backend = @admin.conninfo_hash.slice(:port, :user, :password).merge(host: @admin.host, dbname: @database_name)
    @password = SecureRandom.hex(32)
    @connection_options = {host: @directory, port: 6432, user: backend.fetch(:user), password: @password,
                           dbname: @database_name, connect_timeout: 3}
    auth_path = File.join(@directory, "users.txt")
    File.write(auth_path, "#{auth_quote(backend.fetch(:user))} #{auth_quote(@password)}\n", perm: 0o600)
    config_path = File.join(@directory, "pgbouncer.ini")
    File.write(config_path, <<~CONFIG, perm: 0o600)
      [databases]
      #{@database_name} = #{backend.filter_map { |key, value| "#{key}=#{config_quote(value)}" if value && !value.empty? }.join(" ")}
      [pgbouncer]
      listen_addr =
      listen_port = 6432
      unix_socket_dir = #{@directory}
      unix_socket_mode = 0700
      auth_type = plain
      auth_file = #{auth_path}
      admin_users = #{backend.fetch(:user)}
      pool_mode = transaction
      default_pool_size = 2
      max_client_conn = 40
      server_round_robin = 1
      server_connect_timeout = 3
      query_wait_timeout = 5
      log_connections = 0
      log_disconnections = 0
      pidfile = #{@directory}/pgbouncer.pid
    CONFIG
    @log_path = File.join(@directory, "pgbouncer.log")
    @pid = Process.spawn(@executable, config_path, out: @log_path, err: [:child, :out], pgroup: true)
    Timeout.timeout(10) do
      loop do
        if Process.waitpid(@pid, Process::WNOHANG)
          @pid = nil
          raise "PgBouncer exited during startup: #{diagnostics}"
        end
        begin
          PG.connect(@connection_options) { |connection| connection.exec("SELECT 1") }
          break
        rescue PG::ConnectionBad
          sleep 0.02
        end
      end
    end
  rescue Exception => error # standard:disable Lint/RescueException
    begin
      close
    rescue => cleanup_error
      warn "PgBouncer startup cleanup failed (#{cleanup_error.class})"
    end
    raise error
  end

  def admin_query(sql)
    PG.connect(connection_options.merge(dbname: "pgbouncer")) { |connection| connection.exec(sql).to_a }
  end

  def close
    stop_process
  ensure
    begin
      if @admin && !@admin.finished? && @database_created
        @admin.exec("DROP DATABASE #{@admin.quote_ident(@database_name)} WITH (FORCE)")
        @database_created = false
      end
    ensure
      @admin&.finish unless @admin&.finished?
      @admin = nil
      FileUtils.remove_entry(@directory) if @directory && File.directory?(@directory)
    end
  end

  private

  def stop_process
    if @pid
      begin
        Process.kill("TERM", @pid)
        Timeout.timeout(5) { Process.waitpid(@pid) }
      rescue Timeout::Error
        Process.kill("KILL", @pid)
        Process.waitpid(@pid)
      rescue Errno::ECHILD, Errno::ESRCH
        nil
      ensure
        @pid = nil
      end
    end
  end

  def config_quote(value)
    "'#{value.gsub("'", "''")}'"
  end

  def auth_quote(value)
    %("#{value.gsub('"', '""')}")
  end

  def diagnostics
    text = File.read(@log_path).lines.last(20).join
    [@password, @admin.conninfo_hash[:password]].compact.reject(&:empty?).each { |secret| text = text.gsub(secret, "[REDACTED]") }
    text
  end
end
