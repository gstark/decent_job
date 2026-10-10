# frozen_string_literal: true

require "socket"

module DecentJob
  # A small client for the systemd sd_notify protocol.
  #
  # {SystemdService} calls it. Each call opens, uses, and closes its own UNIX
  # datagram socket, so the module holds no state and any thread can call
  # it. Sends are nonblocking: a full manager queue fails at once instead of
  # delaying shutdown. NOTIFY_SOCKET can be an absolute path or an @abstract
  # name (Linux abstract namespace).
  #
  # Unlike {SystemdService}, these methods raise {NotifyError}.
  #
  # See docs/health-services.md.
  module SdNotify
    # Wraps any error from a send, with the original class in the message.
    class NotifyError < StandardError; end

    class << self
      # Sends one notification message to the service manager.
      #
      # @param state [String] newline-separated KEY=VALUE assignments
      # @param unset_env [Boolean] delete NOTIFY_SOCKET from ENV after the
      #   call, also when NOTIFY_SOCKET is missing or the send fails
      # @return [Integer, nil] bytes sent, or nil when NOTIFY_SOCKET is not set
      # @raise [NotifyError] when NOTIFY_SOCKET is invalid or the send fails
      def notify(state, unset_env = false)
        address = ENV["NOTIFY_SOCKET"]
        return nil unless address

        if !address.start_with?("/") && (!address.start_with?("@") || address.bytesize <= 1)
          raise NotifyError, "NOTIFY_SOCKET must be an absolute path or @abstract name"
        end

        address = "\0#{address.delete_prefix("@")}" if address.start_with?("@")
        socket = Socket.new(Socket::AF_UNIX, Socket::SOCK_DGRAM, 0)
        socket.sendmsg_nonblock(String(state), 0, Socket.sockaddr_un(address))
      rescue => error
        raise NotifyError, "#{error.class}: #{error.message}", error.backtrace
      ensure
        socket&.close
        ENV.delete("NOTIFY_SOCKET") if unset_env
      end

      # Sends READY=1. See {.notify} for the parameter, return, and errors.
      def ready(unset_env = false)
        notify("READY=1", unset_env)
      end

      # Sends STOPPING=1. See {.notify} for the parameter, return, and errors.
      def stopping(unset_env = false)
        notify("STOPPING=1", unset_env)
      end

      # Sends WATCHDOG=1. See {.notify} for the parameter, return, and errors.
      def watchdog(unset_env = false)
        notify("WATCHDOG=1", unset_env)
      end

      # Sends RELOADING=1. See {.notify} for the parameter, return, and errors.
      def reloading(unset_env = false)
        notify("RELOADING=1", unset_env)
      end

      # Sends STATUS=message. See {.notify}.
      #
      # @param message [String] free-form status text for systemctl status
      def status(message, unset_env = false)
        notify("STATUS=#{message}", unset_env)
      end

      # Sends ERRNO=number. See {.notify}.
      #
      # @param number [Integer] an errno value
      def errno(number, unset_env = false)
        notify("ERRNO=#{number}", unset_env)
      end

      # Sends MAINPID=pid. See {.notify}.
      #
      # @param pid [Integer] the service main pid
      def mainpid(pid, unset_env = false)
        notify("MAINPID=#{pid}", unset_env)
      end

      # @return [Boolean] true when the manager requests watchdog pings from
      #   this process
      def watchdog?
        watchdog_interval.positive?
      end

      # Returns the watchdog interval the manager requested.
      #
      # Callers ping at half this interval.
      #
      # @return [Float] seconds, or 0.0 when WATCHDOG_USEC is missing,
      #   invalid, or zero, or when WATCHDOG_PID names another process. This
      #   follows the sd_watchdog_enabled contract.
      def watchdog_interval
        pid = ENV["WATCHDOG_PID"]
        return 0.0 if pid && pid != Process.pid.to_s
        value = ENV["WATCHDOG_USEC"]
        return 0.0 unless value&.match?(/\A[0-9]+\z/)
        microseconds = Integer(value, 10)
        return 0.0 if microseconds > Float::MAX
        interval = microseconds / 1_000_000.0
        (microseconds.positive? && interval.finite?) ? interval : 0.0
      end
    end
  end
end
