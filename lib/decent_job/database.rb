# frozen_string_literal: true

require "pg"

module DecentJob
  class ConnectionTimeout < StandardError; end
  class ClosedDatabase < StandardError; end
  class ForkInProgress < StandardError; end
  class ForkedDatabase < StandardError; end
  class TransactionAborted < StandardError; end

  # A bounded pool. A checkout belongs to one thread until its block returns.
  # Nested operations reuse that checkout, including transactions and locks.
  class Database
    @registry_mutex = Mutex.new
    @registry = ObjectSpace::WeakMap.new

    class << self
      def register(database)
        @registry_mutex.synchronize { @registry[database] = true }
      end

      def with_prepared_fork
        @registry_mutex.synchronize do
          databases = @registry.keys
          prepared = []
          begin
            databases.each do |database|
              database.prepare_fork
              prepared << database
            end
            yield
          ensure
            prepared.each(&:resume_after_fork) if Process.pid == prepared.first&.owner_pid
          end
        end
      end

      def reset_after_fork
        @registry_mutex = Mutex.new
        @registry.keys.each(&:reset_after_fork)
      end
    end

    attr_reader :owner_pid

    def prepare_fork
      raise ForkedDatabase, "create a new Database after forking" if @pid != Process.pid
      @mutex.synchronize do
        raise ForkInProgress, "database has borrowed or connecting sessions" unless @size == @idle.size
        @fork_prepared = true
        @idle.each { |connection| connection.finish unless connection.finished? }
        @idle.clear
        @size = 0
      end
    end

    def resume_after_fork
      @mutex.synchronize { @fork_prepared = false }
    end

    def reset_after_fork
      raise ForkInProgress, "pool must be prepared in the parent before resetting in its child" unless @fork_prepared && @pid != Process.pid
      # Parent preparation closed every owned connection before the fork. Never
      # send PostgreSQL termination packets from an inherited child connection.
      @mutex = Mutex.new
      @available = ConditionVariable.new
      @borrowed = {}
      @transaction_callbacks = {}
      @idle = []
      @size = 0
      @fork_prepared = false
      @pid = @owner_pid = Process.pid
    end

    def resize(pool_size)
      raise ArgumentError, "pool_size must be positive" unless pool_size.is_a?(Integer) && pool_size.positive?
      @mutex.synchronize do
        @pool_size = pool_size
        while @size > @pool_size && (connection = @idle.pop)
          connection.finish unless connection.finished?
          @size -= 1
        end
        @available.broadcast
      end
    end

    def initialize(url:, pool_size: 5, checkout_timeout: 5)
      raise ArgumentError, "pool_size must be positive" unless pool_size.is_a?(Integer) && pool_size.positive?
      raise ArgumentError, "checkout_timeout must be positive" unless checkout_timeout.positive?

      @url = url
      @pool_size = pool_size
      @checkout_timeout = checkout_timeout
      @mutex = Mutex.new
      @available = ConditionVariable.new
      @idle = []
      @borrowed = {}
      @transaction_callbacks = {}
      @size = 0
      @closed = false
      @pid = @owner_pid = Process.pid
      Database.register(self)
    end

    def with_connection
      raise ForkedDatabase, "create a new Database after forking" if @pid != Process.pid

      nested = @mutex.synchronize { @borrowed[Thread.current] }
      return yield nested if nested

      connection = checkout
      begin
        yield connection
      ensure
        checkin(connection)
      end
    end

    def transaction
      with_connection do |connection|
        nested = connection.transaction_status != PG::PQTRANS_IDLE
        savepoint = "decent_job_#{Thread.current.object_id}_#{rand(1 << 32)}"
        connection.exec(nested ? "SAVEPOINT #{savepoint}" : "BEGIN")
        frames = @mutex.synchronize { @transaction_callbacks[Thread.current] ||= [] }
        callbacks = {commit: [], rollback: []}
        frames << callbacks
        committed = false
        begin
          result = yield connection
          raise TransactionAborted, "PostgreSQL transaction was aborted" if connection.transaction_status == PG::PQTRANS_INERROR
          connection.exec(nested ? "RELEASE SAVEPOINT #{savepoint}" : "COMMIT")
          committed = true
          result
        ensure
          begin
            unless committed || connection.finished? || connection.status != PG::CONNECTION_OK
              connection.exec(nested ? "ROLLBACK TO SAVEPOINT #{savepoint}" : "ROLLBACK")
              connection.exec("RELEASE SAVEPOINT #{savepoint}") if nested
            end
          ensure
            frames.pop
            @mutex.synchronize { @transaction_callbacks.delete(Thread.current) } if frames.empty?
          end
          if committed
            if frames.empty?
              callbacks[:commit].each(&:call)
            else
              frames.last[:commit].concat(callbacks[:commit])
              frames.last[:rollback].concat(callbacks[:rollback])
            end
          else
            callbacks[:rollback].each(&:call)
          end
        end
      end
    end

    def after_commit(&block)
      frames = @mutex.synchronize { @transaction_callbacks[Thread.current] }
      if frames && !frames.empty?
        frames.last[:commit] << block
      else
        block.call
      end
    end

    # Savepoint commits retain these callbacks until the outer transaction ends.
    # Without an active transaction there is no rollback to observe.
    def after_rollback(&block)
      frames = @mutex.synchronize { @transaction_callbacks[Thread.current] }
      frames.last[:rollback] << block if frames && !frames.empty?
    end

    # An independently owned connection for LISTEN and other session state.
    # The caller must close it. It does not consume a pool checkout.
    def connect
      raise ForkedDatabase, "create a new Database after forking" if @pid != Process.pid
      PG.connect(@url)
    end

    def stats
      @mutex.synchronize { {size: @pool_size, connections: @size, active: @borrowed.size, idle: @idle.size} }
    end

    # Busy connections close when their current owner returns them.
    def close
      @mutex.synchronize do
        @closed = true
        @idle.each { |connection| connection.finish unless connection.finished? }
        @size -= @idle.size
        @idle.clear
        @available.broadcast
      end
    end

    private

    def checkout
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @checkout_timeout
      reserved = false
      registered = false
      @mutex.synchronize do
        loop do
          raise ForkInProgress, "database is preparing to fork" if @fork_prepared
          raise ClosedDatabase, "database is closed" if @closed

          connection = @idle.pop
          if connection
            @borrowed[Thread.current] = connection
            return connection
          elsif @size < @pool_size
            @size += 1
            reserved = true
            break
          end

          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise ConnectionTimeout, "database checkout timed out" unless remaining.positive?
          @available.wait(@mutex, remaining)
        end
      end
      connection = connect
      @mutex.synchronize do
        raise ClosedDatabase, "database is closed" if @closed
        @borrowed[Thread.current] = connection
        registered = true
      end
      connection
    ensure
      if reserved && !registered
        connection&.finish unless connection&.finished?
        @mutex.synchronize do
          @size -= 1
          @available.signal
        end
      end
    end

    def checkin(connection)
      @mutex.synchronize do
        @borrowed.delete(Thread.current)
        if @closed || @size > @pool_size || connection.finished? || connection.status != PG::CONNECTION_OK
          connection.finish unless connection.finished?
          @size -= 1
        else
          @idle << connection
        end
        @available.signal
      end
    end
  end
end
