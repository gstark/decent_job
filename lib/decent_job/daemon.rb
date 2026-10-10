# frozen_string_literal: true

module DecentJob
  # The locked descriptor survives native daemonization. Supervised children
  # close their copies without LOCK_UN, which would release the parent's lock.
  class Daemon
    class AlreadyRunning < StandardError; end

    attr_reader :pidfile

    def initialize(configuration: DecentJob.configuration, pidfile: configuration.pidfile)
      validated = configuration.dup
      validated.pidfile = pidfile
      @pidfile = validated.pidfile
      @configuration = configuration
    end

    def self.after_fork!
      ObjectSpace.each_object(self, &:close_inherited_descriptor)
    end

    def daemonize
      return self if @file && @owner_pid == Process.pid
      close_inherited_descriptor
      raise "native daemonization is unavailable on this platform" unless Process.respond_to?(:daemon)
      raise ArgumentError, "PID file directory does not exist: #{File.dirname(pidfile)}" unless Dir.exist?(File.dirname(pidfile))
      acquire_pidfile

      DecentJob.with_prepared_fork do
        Process.daemon(true)
        @owner_pid = Process.pid
        write_pid
        at_exit { cleanup }
        DecentJob.after_supervisor_fork(@configuration)
      end

      self
    rescue NotImplementedError
      cleanup
      raise "native daemonization is unavailable on this platform"
    rescue Exception # standard:disable Lint/RescueException
      cleanup
      raise
    end

    def cleanup
      return unless @file

      if @owner_pid == Process.pid && same_file? && read_pid == @owner_pid
        File.unlink(pidfile)
      end
    ensure
      @file&.close unless @file&.closed?
      @file = nil
      @owner_pid = nil
    end

    def close_inherited_descriptor
      return if @owner_pid == Process.pid
      @file&.close unless @file&.closed?
      @file = nil
    end

    private

    def acquire_pidfile
      flags = File::RDWR | File::CREAT
      flags |= File::NOFOLLOW if defined?(File::NOFOLLOW)
      @file = File.open(pidfile, flags, 0o644)
      stat = @file.stat

      unless stat.file? && stat.nlink == 1 && same_file?
        raise ArgumentError, "PID file must be a regular file with one link"
      end

      unless @file.flock(File::LOCK_EX | File::LOCK_NB)
        raise AlreadyRunning, "A daemon already owns PID file #{pidfile}"
      end

      raise ArgumentError, "PID file was replaced during startup" unless same_file?
      pid = read_pid
      raise AlreadyRunning, "A process is already running with PID #{pid}; check #{pidfile}" if pid && live_pid?(pid)
    rescue Errno::ELOOP
      raise ArgumentError, "PID file must not be a symbolic link"
    end

    def read_pid
      @file.rewind
      value = @file.read(128).to_s.strip
      return unless @file.eof?
      return unless value.match?(/\A[0-9]+\z/)
      pid = value.to_i
      pid if pid.positive? && pid <= 2_147_483_647
    end

    def live_pid?(pid)
      Process.kill(0, pid)
      true
    rescue Errno::ESRCH, RangeError
      false
    rescue Errno::EPERM
      true
    end

    def same_file?
      stat = File.lstat(pidfile)
      owned = @file.stat
      stat.file? && stat.nlink == 1 && stat.dev == owned.dev && stat.ino == owned.ino
    rescue Errno::ENOENT
      false
    end

    def write_pid
      raise ArgumentError, "PID file was replaced during startup" unless same_file?
      @file.rewind
      @file.write(@owner_pid.to_s)
      @file.truncate(@file.pos)
      @file.flush
    end
  end
end
