# frozen_string_literal: true

module DecentJob
  # Detaches the CLI process with Process.daemon and owns its PID file.
  #
  # {CLI} creates a daemon when {Configuration#daemonize} is true and calls
  # {#daemonize} before it starts workers or the {Supervisor}. The detached
  # process then runs the single worker or the supervisor.
  #
  # Ownership: the detached daemon process (the owner pid) holds an open
  # descriptor on the PID file with an exclusive flock for its whole life.
  # The lock, not the file contents, is what stops a second launch. The
  # locked descriptor survives native daemonization because Process.daemon
  # keeps open descriptors other than stdio.
  #
  # Constraints: forked children (supervised workers) inherit the descriptor.
  # They must close their copies with {#close_inherited_descriptor} and must
  # never call LOCK_UN, because LOCK_UN on any copy releases the lock for the
  # whole open file description, including the parent's. A plain close keeps
  # the lock while the parent's copy stays open.
  #
  # Invariants: the PID file must be a regular file with one link, never a
  # symbolic link. Each write and the final unlink check the path still names
  # the locked inode, so a replaced file stays untouched.
  #
  # See docs/daemon-and-idle.md.
  class Daemon
    # Raised when another live daemon holds the lock or a live pid is in the file.
    class AlreadyRunning < StandardError; end

    # @return [String] the absolute PID file path
    attr_reader :pidfile

    # @param configuration [Configuration] passed to
    #   {DecentJob.after_supervisor_fork} in the detached process
    # @param pidfile [String, #to_path] PID file path. Relative paths expand
    #   against the current directory.
    # @raise [ArgumentError] when the path is empty or contains a NUL byte
    def initialize(configuration: DecentJob.configuration, pidfile: configuration.pidfile)
      validated = configuration.dup
      validated.pidfile = pidfile
      @pidfile = validated.pidfile
      @configuration = configuration
    end

    # Closes inherited PID-file descriptors in every Daemon object of a new
    # child process.
    #
    # @return [Integer] the number of Daemon objects visited
    # @note {DecentJob.after_supervisor_fork} calls this. The daemon process
    #   itself also runs it, after it sets its owner pid, so it keeps its lock.
    # @api private
    def self.after_fork!
      ObjectSpace.each_object(self, &:close_inherited_descriptor)
    end

    # Takes the PID file lock, detaches, and writes the new pid.
    #
    # The launching process exits with status 0 inside Process.daemon. Only
    # the detached process returns. It keeps the working directory, and its
    # stdin, stdout, and stderr point to /dev/null.
    #
    # @return [Daemon] self. A second call in the owner process does nothing.
    # @raise [AlreadyRunning] when another daemon holds the lock or a live pid
    #   is in the file. A pid the caller cannot signal (EPERM) counts as live.
    # @raise [ArgumentError] when the PID file directory does not exist, the
    #   path is a symbolic link, the file has more than one link, or the file
    #   was replaced during startup
    # @raise [RuntimeError] when the platform has no native daemonization
    # @note Forks through {DecentJob.with_prepared_fork}, so registered
    #   workers and cron managers drain and before_supervisor_fork hooks run
    #   first. Registers an at_exit handler that calls {#cleanup}.
    # @note On any failure it calls {#cleanup} and re-raises.
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

    # Releases the PID file. The owner process also deletes the file.
    #
    # The file is deleted only when this process is the owner, the path still
    # names the locked inode, and the file still holds the owner pid.
    #
    # @return [void]
    # @note Safe to call more than once. {CLI} calls it before a forced exit
    #   after a shutdown timeout, and the at_exit handler calls it again.
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

    # Closes this object's PID-file descriptor unless this process is the owner.
    #
    # @return [void]
    # @note Never unlocks or deletes the file. See the class header for why.
    def close_inherited_descriptor
      return if @owner_pid == Process.pid
      @file&.close unless @file&.closed?
      @file = nil
    end

    private

    # Opens with O_NOFOLLOW where available, then checks the inode again after
    # flock to detect a file replaced between open and lock. Invalid contents
    # and dead pids can be reclaimed.
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

    # Returns nil for contents that are not one bounded positive integer. The
    # eof check rejects a file longer than the 128 bytes read.
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

    # Compares the path's lstat with the open descriptor's stat, so a renamed,
    # replaced, or hard-linked path does not match.
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
