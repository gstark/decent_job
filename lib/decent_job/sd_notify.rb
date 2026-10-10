# frozen_string_literal: true

require "socket"

module DecentJob
  # A small sd_notify protocol client; each call owns and closes its datagram
  # socket. A full manager queue fails immediately instead of delaying shutdown.
  module SdNotify
    class NotifyError < StandardError; end

    class << self
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

      def ready(unset_env = false)
        notify("READY=1", unset_env)
      end

      def stopping(unset_env = false)
        notify("STOPPING=1", unset_env)
      end

      def watchdog(unset_env = false)
        notify("WATCHDOG=1", unset_env)
      end

      def reloading(unset_env = false)
        notify("RELOADING=1", unset_env)
      end

      def status(message, unset_env = false)
        notify("STATUS=#{message}", unset_env)
      end

      def errno(number, unset_env = false)
        notify("ERRNO=#{number}", unset_env)
      end

      def mainpid(pid, unset_env = false)
        notify("MAINPID=#{pid}", unset_env)
      end

      def watchdog?
        watchdog_interval.positive?
      end

      # Seconds requested by the manager; services send at half this interval.
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
