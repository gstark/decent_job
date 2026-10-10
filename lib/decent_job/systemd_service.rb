# frozen_string_literal: true

require "decent_job/sd_notify"

module DecentJob
  # Sends systemd READY and STOPPING notifications and runs the watchdog
  # thread for one service lifecycle.
  #
  # {CLI} creates one in the CLI process or the supervisor. Children never
  # run one. Applications that manage their own lifecycle can use it
  # directly. It does nothing visible when systemd did not set NOTIFY_SOCKET
  # or WATCHDOG_USEC.
  #
  # Ownership: the object owns at most one watchdog thread. A mutex guards
  # its flags. The generation counter makes a stale watchdog thread from an
  # earlier {#start} exit instead of pinging for a later one.
  #
  # Constraints: notification errors are reported to on_thread_error and the
  # systemd_watchdog_error.decent_job event, then swallowed, so manager
  # failures cannot interrupt worker draining. The lower-level {SdNotify} API
  # raises NotifyError.
  #
  # See docs/health-services.md.
  class SystemdService
    # @param on_thread_error [#call, nil] receives notification errors
    def initialize(on_thread_error: DecentJob.configuration.on_thread_error)
      @on_thread_error = on_thread_error
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @active = false
      @stopping = false
      @drains = 0
      @generation = 0
    end

    # Sends READY=1 and starts the watchdog thread when the manager asks for one.
    #
    # @return [SystemdService] self. Does nothing when already active or while
    #   {#stop} runs.
    # @note The watchdog thread sends WATCHDOG=1 every half interval and emits
    #   systemd_watchdog_start.decent_job with the interval in seconds.
    def start
      generation = @mutex.synchronize do
        return self if @active || @drains.positive?
        @active = true
        @generation += 1
        @starting = Thread.current
        @stopping = false
        @watchdog_stop = false
        @generation
      end

      safely { SdNotify.ready }
      interval = SdNotify.watchdog_interval / 2

      if interval.positive?
        safely { Notifications.instrument("systemd_watchdog_start.decent_job", interval: interval) }

        @mutex.synchronize do
          if !@stopping && @active && @generation == generation
            @watchdog = Thread.new { run_watchdog(interval, generation) }
          end
        end
      end

      self
    ensure
      @mutex.synchronize do
        if @starting == Thread.current && @generation == generation
          @starting = nil
          @condition.broadcast
        end
      end
    end

    # Sends STOPPING=1 once. The watchdog keeps running.
    #
    # Waits for a {#start} in progress on another thread, so STOPPING never
    # comes before READY.
    #
    # @return [SystemdService] self
    def stopping
      notify = @mutex.synchronize do
        @condition.wait(@mutex) while @starting && @starting != Thread.current
        next false if @stopping
        @stopping = true
      end

      safely { SdNotify.stopping } if notify
      self
    end

    # Announces STOPPING, runs the optional drain block, then ends the watchdog.
    #
    # The watchdog keeps pinging while the block runs, so a long drain does
    # not trip the manager's watchdog. The watchdog also stops when the block
    # raises.
    #
    # @yield the drain work, for example DecentJob.shutdown
    # @return [SystemdService] self
    # @note Nested or concurrent calls share one watchdog. The last call to
    #   finish stops and joins it.
    def stop
      generation = @mutex.synchronize {
        @drains += 1
        @generation
      }

      begin
        stopping
        yield if block_given?
      ensure
        thread, last = @mutex.synchronize do
          @drains -= 1

          if @drains.zero?
            @watchdog_stop = true
            @condition.broadcast
            [@watchdog, true]
          else
            [nil, false]
          end
        end

        begin
          thread.join if thread && thread != Thread.current
        ensure
          @mutex.synchronize do
            if last && @drains.zero? && @generation == generation
              @watchdog = nil
              @active = false
            end
          end
        end
      end

      self
    end

    # @return [Boolean] true while the watchdog thread is alive and not told to stop
    def notifying?
      @mutex.synchronize { !!(@watchdog&.alive? && !@watchdog_stop) }
    end

    private

    def run_watchdog(interval, generation)
      deadline = monotonic_time + interval

      loop do
        stopped = @mutex.synchronize do
          loop do
            break if @watchdog_stop || @generation != generation
            remaining = deadline - monotonic_time
            break unless remaining.positive?
            @condition.wait(@mutex, [remaining, 3600].min)
          end

          @watchdog_stop || @generation != generation
        end

        break if stopped
        safely { SdNotify.watchdog }
        deadline = monotonic_time + interval
      end
    end

    # Never raises: a failure here must not interrupt startup or drain.
    def safely
      yield
    rescue Exception => error # standard:disable Lint/RescueException
      begin
        Notifications.instrument("systemd_watchdog_error.decent_job", error: error)
      rescue Exception # standard:disable Lint/RescueException
        # Subscribers are application callbacks too.
      end

      begin
        @on_thread_error&.call(error)
      rescue Exception # standard:disable Lint/RescueException
        nil
      end
    end

    def monotonic_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
