# frozen_string_literal: true

require "decent_job/sd_notify"

module DecentJob
  # Notification errors are reported and swallowed so manager failures cannot
  # interrupt worker draining. The lower-level SdNotify API raises NotifyError.
  class SystemdService
    def initialize(on_thread_error: DecentJob.configuration.on_thread_error)
      @on_thread_error = on_thread_error
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @active = false
      @stopping = false
      @drains = 0
      @generation = 0
    end

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

    def stopping
      notify = @mutex.synchronize do
        @condition.wait(@mutex) while @starting && @starting != Thread.current
        next false if @stopping
        @stopping = true
      end

      safely { SdNotify.stopping } if notify
      self
    end

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
