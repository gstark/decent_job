# frozen_string_literal: true

require "pg"

module DecentJob
  class ConnectionTimeout < StandardError; end
  class ClosedDatabase < StandardError; end
  class ForkedDatabase < StandardError; end
  class TransactionAborted < StandardError; end

  # A bounded pool. A checkout belongs to one thread until its block returns.
  # Nested operations reuse that checkout, including transactions and locks.
  class Database
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
      @pid = Process.pid
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
        callbacks = []
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
              callbacks.each(&:call)
            else
              frames.last.concat(callbacks)
            end
          end
        end
      end
    end

    def after_commit(&block)
      frames = @mutex.synchronize { @transaction_callbacks[Thread.current] }
      if frames && !frames.empty?
        frames.last << block
      else
        block.call
      end
    end

    # An independently owned connection for LISTEN and other session state.
    # The caller must close it. It does not consume a pool checkout.
    def connect
      PG.connect(@url)
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
        if @closed || connection.finished? || connection.status != PG::CONNECTION_OK
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
