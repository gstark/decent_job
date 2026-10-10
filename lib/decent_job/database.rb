# frozen_string_literal: true

require "pg"

module DecentJob
  # Raised when no pool connection becomes free within checkout_timeout.
  class ConnectionTimeout < StandardError; end
  # Raised on checkout after {Database#close}.
  class ClosedDatabase < StandardError; end
  # Raised when a pool is preparing to fork, or cannot prepare because a
  # connection is in use.
  class ForkInProgress < StandardError; end
  # Raised when a forked child uses a pool that it has not reset.
  class ForkedDatabase < StandardError; end
  # Raised when a transaction block returns while PostgreSQL has already
  # aborted the transaction, for example after the block rescued a PG error.
  class TransactionAborted < StandardError; end

  # A bounded pool. A checkout belongs to one execution context until its block returns.
  # Nested operations reuse that checkout, including transactions and locks.
  #
  # The execution context is {IsolatedState.context}: the current Thread, or
  # the Active Support isolation context when Active Support is loaded. Code
  # that holds a session advisory lock ({Claim}) depends on this reuse: a
  # nested call must see the same PostgreSQL session.
  #
  # Interrupt rules: checkout and checkin are protected from Thread#raise and
  # Thread#kill. The caller's block stays interruptible. When the owning
  # thread is killed, checkin cancels any running query and closes the
  # session instead of returning it to the pool. A killed PG query can leave
  # unread protocol data, and the session can hold locks that nobody owns.
  #
  # Fork rules: every instance registers itself in a class-level registry.
  # {Database.with_prepared_fork} closes all idle connections before a fork,
  # and {Database.reset_after_fork} gives the child fresh, empty pools. A child
  # must never send a termination packet on a socket that the parent owns.
  # Connections from {#connect} are not in the pool, so fork preparation and
  # {#close} do not close them.
  #
  # See docs/runtime-resources.md and docs/pgbouncer.md.
  class Database
    @registry_mutex = Mutex.new
    @registry = ObjectSpace::WeakMap.new

    class << self
      # Adds database to the fork registry. Called from {Database#initialize}.
      #
      # @param database [Database]
      # @return [void]
      # @raise [ForkInProgress] while {.with_prepared_fork} is running
      # @api private
      def register(database)
        @registry_mutex.synchronize do
          raise ForkInProgress, "databases are preparing to fork" if @fork_prepared

          @registry[database] = true
        end
      end

      # Prepares every registered pool for a fork, yields, then resumes the
      # pools in the parent.
      #
      # Fork inside the block. New registrations and checkouts fail until the
      # block returns. The parent resumes every pool that it started to
      # prepare, also when preparation fails. The child skips the resume and
      # must call {.reset_after_fork}.
      #
      # @yield the fork
      # @return [Object] the block result
      # @raise [ForkInProgress] when another preparation runs, or a pool has a
      #   borrowed or connecting connection
      # @raise [ForkedDatabase] when a registered pool belongs to another process
      # @note Closes all idle pooled connections.
      # @api private
      def with_prepared_fork
        owner = Process.pid
        acquired = false
        databases = nil
        prepared = []

        Thread.handle_interrupt(Object => :never) do
          @registry_mutex.synchronize do
            raise ForkInProgress, "databases are already preparing to fork" if @fork_prepared

            @fork_prepared = true
            acquired = true
            databases = @registry.keys
          end
        end

        databases.each do |database|
          Thread.handle_interrupt(Object => :never) do
            # Register cleanup before preparation can close sessions or fail.
            prepared << database
            database.prepare_fork
          end
        end

        yield
      ensure
        if acquired && Process.pid == owner
          Thread.handle_interrupt(Object => :never) do
            prepared.each(&:resume_after_fork)
          ensure
            @registry_mutex.synchronize { @fork_prepared = false }
          end
        end
      end

      # Resets the registry and every registered pool in a forked child.
      # Replaces the mutex, because the parent could hold it at fork time.
      #
      # @return [void]
      # @raise [ForkInProgress] when a pool was not prepared in the parent
      # @note Call only in the child, after a fork inside {.with_prepared_fork}.
      # @api private
      def reset_after_fork
        @registry_mutex = Mutex.new
        @fork_prepared = false
        @registry.keys.each(&:reset_after_fork)
      end
    end

    # @return [Integer] the pid of the process that owns this pool
    attr_reader :owner_pid

    # Closes idle connections and blocks checkouts before a fork.
    #
    # @return [void]
    # @raise [ForkedDatabase] when this pool belongs to another process
    # @raise [ForkInProgress] when a connection is borrowed or connecting
    # @api private
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

    # Allows checkouts again in the parent after a fork.
    #
    # @return [void]
    # @api private
    def resume_after_fork
      @mutex.synchronize { @fork_prepared = false }
    end

    # Replaces all pool state in a forked child and makes the child the owner.
    #
    # @return [void]
    # @raise [ForkInProgress] when the pool was not prepared, or this is not a
    #   child process
    # @api private
    def reset_after_fork
      raise ForkInProgress, "pool must be prepared in the parent before resetting in its child" unless @fork_prepared && @pid != Process.pid

      # Parent preparation closed every owned connection before the fork. Never
      # send PostgreSQL termination packets from an inherited child connection.
      @mutex = Mutex.new
      @available = ConditionVariable.new
      @borrowed = {}
      @transaction_callbacks = {}
      @connection_release_callbacks = {}
      @idle = []
      @size = 0
      @fork_prepared = false
      @pid = @owner_pid = Process.pid
    end

    # Changes the maximum number of pooled connections.
    #
    # A smaller size closes surplus idle connections now. Surplus borrowed
    # connections close when their owners check them in.
    #
    # @param pool_size [Integer] the new maximum, greater than zero
    # @return [void]
    # @raise [ArgumentError] when pool_size is not a positive Integer
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

    # Creates an empty pool. Connections open on demand.
    #
    # @param url [String] the PostgreSQL connection string for PG.connect
    # @param pool_size [Integer] the maximum number of pooled connections
    # @param checkout_timeout [Numeric] seconds to wait for a free connection
    # @raise [ArgumentError] when pool_size or checkout_timeout is not positive
    # @raise [ForkInProgress] while {.with_prepared_fork} is running
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
      @connection_release_callbacks = {}
      @size = 0
      @closed = false
      @pid = @owner_pid = Process.pid
      Database.register(self)
    end

    # Yields the current context's connection, checking one out if needed.
    #
    # A nested call yields the same connection and does not check it in. The
    # outermost call checks it in when its block ends.
    #
    # @yieldparam connection [PG::Connection] the checked-out connection. Do
    #   not keep it after the block returns.
    # @return [Object] the block result
    # @raise [ConnectionTimeout] when no connection is free within
    #   checkout_timeout seconds
    # @raise [ClosedDatabase] after {#close}
    # @raise [ForkInProgress] while a fork is being prepared
    # @raise [ForkedDatabase] in a forked child before {.reset_after_fork}
    # @note Checkin runs the {#after_connection_release} callbacks.
    def with_connection
      raise ForkedDatabase, "create a new Database after forking" if @pid != Process.pid

      owner = IsolatedState.context
      nested = @mutex.synchronize { @borrowed[owner] }
      return yield nested if nested

      # Protect ownership handoffs, while pool waits/connect and application
      # work remain interruptible. Ensure also covers checkout's return boundary.
      Thread.handle_interrupt(Object => :on_blocking) do
        connection = checkout(owner: owner)
        Thread.handle_interrupt(Object => :immediate) { yield connection }
      ensure
        Thread.handle_interrupt(Object => :never) do
          connection ||= @mutex.synchronize { @borrowed[owner] }
          # A killed PG query can leave unread protocol data despite CONNECTION_OK.
          # Retire only after application ensure blocks have released ownership.
          checkin(connection, owner: owner, interrupted: Thread.current.status == "aborting") if connection
        end
      end
    end

    # Runs the block in a transaction, or in a savepoint when the connection
    # is already in a transaction.
    #
    # The transaction commits when the block returns and rolls back when it
    # raises. If the thread is killed, no COMMIT or ROLLBACK is sent;
    # {#with_connection} closes the session, which aborts the transaction.
    # A committed savepoint passes its callbacks to the enclosing frame.
    # Commit callbacks run after the outermost COMMIT. Rollback callbacks run
    # when their own frame or any enclosing frame rolls back.
    #
    # @yieldparam connection [PG::Connection] the checked-out connection
    # @return [Object] the block result, or nil when the thread is aborting
    # @raise [TransactionAborted] when the block returns but PostgreSQL has
    #   aborted the transaction
    # @note The savepoint check reads transaction_status. A connection in an
    #   aborted transaction gets a SAVEPOINT command, which fails.
    def transaction
      with_connection do |connection|
        Thread.handle_interrupt(Object => :on_blocking) do
          nested = connection.transaction_status != PG::PQTRANS_IDLE
          savepoint = "decent_job_#{Thread.current.object_id}_#{rand(1 << 32)}"
          frames = @mutex.synchronize { @transaction_callbacks[connection] ||= [] }
          callbacks = {commit: [], rollback: []}
          frames << callbacks
          committed = false

          begin
            connection.exec(nested ? "SAVEPOINT #{savepoint}" : "BEGIN")
            result = Thread.handle_interrupt(Object => :immediate) { yield connection }
            # A handler may rescue an error raised by killed cleanup. Never
            # commit that thread's unfinished transaction or release its savepoint.
            next if Thread.current.status == "aborting"

            raise TransactionAborted, "PostgreSQL transaction was aborted" if connection.transaction_status == PG::PQTRANS_INERROR

            connection.exec(nested ? "RELEASE SAVEPOINT #{savepoint}" : "COMMIT")
            committed = true
            result
          ensure
            begin
              unless committed || Thread.current.status == "aborting" || connection.finished? || connection.status != PG::CONNECTION_OK
                connection.exec(nested ? "ROLLBACK TO SAVEPOINT #{savepoint}" : "ROLLBACK")
                connection.exec("RELEASE SAVEPOINT #{savepoint}") if nested
              end
            ensure
              frames.pop
              @mutex.synchronize { @transaction_callbacks.delete(connection) } if frames.empty?
            end

            if committed
              if frames.empty?
                Thread.handle_interrupt(Object => :immediate) { callbacks[:commit].each(&:call) }
              else
                frames.last[:commit].concat(callbacks[:commit])
                frames.last[:rollback].concat(callbacks[:rollback])
              end
            else
              Thread.handle_interrupt(Object => :immediate) { callbacks[:rollback].each(&:call) }
            end
          end
        end
      end
    end

    # Inspect only this context's checkout. An aborted transaction cannot accept
    # a savepoint; observers must still run so they can report the original error.
    #
    # @return [Boolean, nil] true when this context's connection is in an open,
    #   healthy transaction. False or nil otherwise, including in an aborted
    #   transaction or with no checkout.
    def in_transaction?
      connection = @mutex.synchronize { @borrowed[IsolatedState.context] }
      connection && !connection.finished? && connection.transaction_status == PG::PQTRANS_INTRANS
    end

    # Runs the block after the outermost {#transaction} of this context
    # commits. Runs it at once when there is no such transaction.
    #
    # @yield the callback
    # @return [void]
    # @note Only transactions opened with {#transaction} count. A raw BEGIN on
    #   the connection does not defer the callback.
    def after_commit(&block)
      frames = @mutex.synchronize { @transaction_callbacks[@borrowed[IsolatedState.context]] }

      if frames && !frames.empty?
        frames.last[:commit] << block
      else
        block.call
      end
    end

    # Savepoint commits retain these callbacks until the outer transaction ends.
    # Without an active transaction there is no rollback to observe.
    #
    # @yield the callback, run when the innermost open {#transaction} frame or
    #   an enclosing frame rolls back
    # @return [void]
    # @note Without an open {#transaction} the block is dropped.
    def after_rollback(&block)
      frames = @mutex.synchronize { @transaction_callbacks[@borrowed[IsolatedState.context]] }
      frames.last[:rollback] << block if frames && !frames.empty?
    end

    # Internal cleanup that must wait until this context releases its outer
    # checkout, including application ensures and retirement of killed sessions.
    #
    # @yield the callback. It runs at once when this context has no checkout.
    # @return [void]
    # @note Callbacks run after the connection is back in the pool or closed.
    #   Every callback runs; the first error is raised after the last one.
    # @api private
    def after_connection_release(&block)
      connection = @mutex.synchronize do
        current = @borrowed[IsolatedState.context]
        (@connection_release_callbacks[current] ||= []) << block if current
        current
      end

      block.call unless connection
    end

    # An independently owned connection for LISTEN and other session state.
    # The caller must close it. It does not consume a pool checkout.
    #
    # @return [PG::Connection] a new connection that the caller owns
    # @raise [ForkedDatabase] in a forked child before {.reset_after_fork}
    # @raise [PG::Error] when the connection fails
    # @note {#close} and fork preparation do not track or close it.
    def connect
      raise ForkedDatabase, "create a new Database after forking" if @pid != Process.pid

      PG.connect(@url)
    end

    # @return [Hash{Symbol => Integer}] size is the configured maximum,
    #   connections counts open and connecting slots, active counts borrowed
    #   connections, and idle counts connections ready for checkout
    def stats
      @mutex.synchronize { {size: @pool_size, connections: @size, active: @borrowed.size, idle: @idle.size} }
    end

    # Busy connections close when their current owner returns them.
    #
    # The pool stays closed. Later checkouts raise {ClosedDatabase}, and
    # waiting checkouts wake up and raise it.
    #
    # @return [void]
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

    # Reserves a slot under the mutex, then connects outside it so a slow
    # connect does not block other checkouts. The ensure frees the slot if the
    # connect fails, is interrupted, or the pool closes during the connect.
    def checkout(owner:)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @checkout_timeout
      reserved = false
      registered = false

      @mutex.synchronize do
        loop do
          raise ForkInProgress, "database is preparing to fork" if @fork_prepared
          raise ClosedDatabase, "database is closed" if @closed

          connection = @idle.pop

          if connection
            @borrowed[owner] = connection
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

        @borrowed[owner] = connection
        registered = true
      end

      connection
    ensure
      if reserved && !registered
        Thread.handle_interrupt(Object => :never) do
          connection&.finish unless connection&.finished?

          @mutex.synchronize do
            @size -= 1
            @available.signal
          end
        end
      end
    end

    # Asks the server to cancel a query that a killed thread left running, so
    # its locks release before the session closes. Waits at most one second.
    def cancel_interrupted_query(connection)
      return if connection.finished? || connection.status != PG::CONNECTION_OK || connection.transaction_status != PG::PQTRANS_ACTIVE

      unless defined?(PG::CancelConnection)
        # Older libpq clients expose only pg's interruptible cancellation API.
        # This remains owned cleanup; shutdown callers need not wait for it.
        connection.async_cancel
        return
      end

      cancellation = PG::CancelConnection.new(connection)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
      cancellation.start

      loop do
        state = cancellation.poll
        return if state == PG::PGRES_POLLING_OK || state == PG::PGRES_POLLING_FAILED

        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        return unless remaining.positive?

        socket = cancellation.socket_io
        readable = (state == PG::PGRES_POLLING_READING) ? [socket] : nil
        writable = (state == PG::PGRES_POLLING_WRITING) ? [socket] : nil
        return unless IO.select(readable, writable, nil, remaining)
      end
    rescue PG::Error, IOError, SystemCallError
      # Cancellation is best effort when the server or network has failed.
      nil
    ensure
      cancellation&.finish
    end

    # Returns the connection to the idle list, or closes it when the owner was
    # killed, the pool is closed or shrunk, or the connection is bad. Release
    # callbacks run after the mutex is released.
    def checkin(connection, owner:, interrupted: false)
      cancel_interrupted_query(connection) if interrupted
    ensure
      callbacks = @mutex.synchronize do
        @borrowed.delete(owner)
        @transaction_callbacks.delete(connection)

        if interrupted || @closed || @size > @pool_size || connection.finished? || connection.status != PG::CONNECTION_OK
          connection.finish unless connection.finished?
          @size -= 1
        else
          @idle << connection
        end

        @available.signal
        @connection_release_callbacks.delete(connection) || []
      end

      failure = nil

      callbacks.each do |callback|
        callback.call
      rescue Exception => error # standard:disable Lint/RescueException
        failure ||= error
      end

      raise failure if failure
    end
  end
end
