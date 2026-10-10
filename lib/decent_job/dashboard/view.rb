# frozen_string_literal: true

require "erb"
require "json"
require "uri"

module DecentJob
  class Dashboard
    # Renders ERB templates from the views directory. Every value that reaches
    # markup goes through +h+; templates never emit raw job data.
    class View
      TEMPLATES = File.expand_path("views", __dir__)
      STATUS_LABELS = {"queued" => "Queued", "scheduled" => "Scheduled", "retried" => "Retrying",
                       "running" => "Running", "succeeded" => "Succeeded", "discarded" => "Discarded"}.freeze

      attr_reader :request, :root, :csrf_token, :flash, :theme

      def initialize(request:, root:, csrf_token:, flash: nil, theme: nil)
        @request = request
        @root = root
        @csrf_token = csrf_token
        @flash = flash
        @theme = theme
        @templates = {}
      end

      def render(name, assigns = {})
        assigns.each { |key, value| instance_variable_set("@#{key}", value) }
        template(name).result(binding)
      end

      def layout(name, assigns = {})
        @page_name = name
        content = render(name, assigns)
        render("layout", content: content)
      end

      # ----- escaping and links -----------------------------------------

      def h(value)
        ERB::Util.html_escape(value.to_s)
      end

      def url(path, params = nil)
        target = "#{root}#{path}"
        return target unless params

        pairs = params.compact.flat_map do |key, value|
          Array(value).map { |item| [value.is_a?(Array) ? "#{key}[]" : key.to_s, item.to_s] }
        end

        pairs.empty? ? target : "#{target}?#{URI.encode_www_form(pairs)}"
      end

      def current_path
        request.path
      end

      def active?(prefix)
        (prefix == "/") ? request.path == "/" : request.path.start_with?(prefix)
      end

      # ----- formatting -------------------------------------------------

      def time(value, relative: true)
        return "" unless value
        iso = value.utc.iso8601
        label = relative ? relative_time(value) : value.utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        %(<time datetime="#{h(iso)}" title="#{h(value.utc.strftime("%Y-%m-%d %H:%M:%S.%3N UTC"))}">#{h(label)}</time>)
      end

      def relative_time(value, now: Time.now)
        seconds = (now - value).round
        past = seconds >= 0
        seconds = seconds.abs

        text = if seconds < 5 then "now"
        elsif seconds < 60 then "#{seconds}s"
        elsif seconds < 3600 then "#{seconds / 60}m"
        elsif seconds < 86_400 then "#{seconds / 3600}h #{(seconds % 3600) / 60}m"
        else "#{seconds / 86_400}d #{(seconds % 86_400) / 3600}h"
        end

        return text if text == "now"
        past ? "#{text} ago" : "in #{text}"
      end

      def duration(seconds)
        return "" if seconds.nil?
        seconds = seconds.to_f

        if seconds < 0.001 then "<1 ms"
        elsif seconds < 1 then "#{(seconds * 1000).round} ms"
        elsif seconds < 60 then "#{seconds.round(2)} s"
        elsif seconds < 3600 then "#{(seconds / 60).floor}m #{(seconds % 60).round}s"
        else "#{(seconds / 3600).floor}h #{((seconds % 3600) / 60).round}m"
        end
      end

      def number(value)
        value.to_i.to_s.reverse.scan(/\d{1,3}/).join(",").reverse
      end

      def status_label(status)
        STATUS_LABELS.fetch(status.to_s, status.to_s)
      end

      def status_badge(status)
        %(<span class="status status-#{h(status)}">#{h(status_label(status))}</span>)
      end

      def short_id(id)
        id.to_s[0, 8]
      end

      def pretty_json(value)
        JSON.pretty_generate(value)
      rescue JSON::GeneratorError
        value.inspect
      end

      def truncate(text, length = 80)
        text = text.to_s
        (text.length > length) ? "#{text[0, length - 1]}…" : text
      end

      INTERNAL_KEYS = [Arguments::RUBY2_KEYWORDS_KEY, Arguments::SYMBOL_KEYS_KEY, Arguments::INDIFFERENT_KEY, Arguments::OBJECT_KEY].freeze

      def arguments_of(job)
        clean_arguments(job.fetch("payload", {}).fetch("arguments", []))
      end

      def argument_preview(job)
        arguments_of(job).map { |argument| JSON.generate(argument) }.join(", ")
      rescue JSON::GeneratorError
        arguments_of(job).inspect
      end

      # Strips Active Job's serialization markers so operators see their own values.
      def clean_arguments(value)
        case value
        when Hash then value.except(*INTERNAL_KEYS).to_h { |key, item| [key, clean_arguments(item)] }
        when Array then value.map { |item| clean_arguments(item) }
        else value
        end
      end

      def error_summary(error)
        return "" unless error.is_a?(Hash)
        "#{error["class"]}: #{truncate(error["message"], 160)}"
      end

      def application_backtrace(lines)
        Array(lines).reject { |line| line.to_s.match?(%r{/gems/|/ruby/\d|<internal:|/lib/decent_job/|/lib/active_job/}) }
      end

      # ----- forms ------------------------------------------------------

      def csrf_field
        %(<input type="hidden" name="authenticity_token" value="#{h(csrf_token)}">)
      end

      def action_button(path, label, params: {}, confirm: nil, kind: "quiet")
        hidden = params.map { |key, value| %(<input type="hidden" name="#{h(key)}" value="#{h(value)}">) }.join
        confirm_attribute = confirm ? %( data-confirm="#{h(confirm)}") : ""
        %(<form method="post" action="#{h(url(path))}" class="inline"#{confirm_attribute}>#{csrf_field}#{hidden}<button class="button #{h(kind)}">#{h(label)}</button></form>)
      end

      private

      def template(name)
        @templates[name] ||= begin
          path = File.join(TEMPLATES, "#{name}.erb")
          raise ArgumentError, "unknown template #{name}" unless File.file?(path)
          ERB.new(File.read(path), trim_mode: "-")
        end
      end
    end
  end
end
