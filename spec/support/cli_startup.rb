# frozen_string_literal: true

require "timeout"

# Runtime logs may precede the CLI readiness marker. Keep them in diagnostics.
module CliStartupSpec
  def await_started(output, error)
    lines = []

    Timeout.timeout(8) do
      while (line = output.gets)
        lines << line
        return lines.join if line.include?("DecentJob started. Send SIGINT or SIGTERM to stop.")
      end

      flunk "CLI failed to boot. stdout: #{lines.join.inspect}; stderr: #{error.read}"
    end
  rescue Timeout::Error
    stderr = error.read_nonblock(4096, exception: false)
    flunk "CLI did not boot within 8 seconds. stdout: #{lines.join.inspect}; stderr: #{stderr.inspect}"
  end
end
