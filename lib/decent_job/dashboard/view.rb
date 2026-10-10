# frozen_string_literal: true

require "erb"
require "json"
require "uri"

module DecentJob
  class Dashboard
    # Renders ERB templates from the views directory. Every value that reaches
    # markup goes through +h+; templates never emit raw job data.
    #
    # Helpers that return markup ({#time}, {#status_badge}, {#csrf_field},
    # {#action_button}) escape their own inputs, so templates emit their
    # result without +h+. Templates emit numbers and fixed strings directly.
    # Any new template output of stored data must use +h+.
    #
    # {Dashboard#call} builds one View per request. Templates run with this
    # object as their binding, so assigns become instance variables on it. Do
    # not share a View across requests or threads.
    #
    # See docs/dashboard.md.
    class View
      TEMPLATES = File.expand_path("views", __dir__)
      STATUS_LABELS = {"queued" => "Queued", "scheduled" => "Scheduled", "retried" => "Retrying",
                       "running" => "Running", "succeeded" => "Succeeded", "discarded" => "Discarded"}.freeze

      # request is the {Request}. root is the mount path ("" at the root).
      # csrf_token is the value forms must post back. flash is the notice from
      # the last redirect, or nil. theme is "light", "dark", or nil for the
      # operating system default.
      attr_reader :request, :root, :csrf_token, :flash, :theme

      # @param request [Request]
      # @param root [String] mount path without a trailing "/"
      # @param csrf_token [String] the token from the decent_job_csrf cookie
      # @param flash [String, nil] notice to show once
      # @param theme [String, nil] "light", "dark", or nil
      def initialize(request:, root:, csrf_token:, flash: nil, theme: nil)
        @request = request
        @root = root
        @csrf_token = csrf_token
        @flash = flash
        @theme = theme
        @templates = {}
      end

      # Renders one template without the layout.
      #
      # @param name [String] template name without ".erb"
      # @param assigns [Hash{Symbol => Object}] set as instance variables
      # @return [String] HTML
      # @raise [ArgumentError] when no template has this name
      def render(name, assigns = {})
        assigns.each { |key, value| instance_variable_set("@#{key}", value) }

        template(name).result(binding)
      end

      # Renders a page template inside layout.erb.
      #
      # @param name [String] page template name, also used for navigation state
      # @param assigns [Hash{Symbol => Object}] set as instance variables
      # @return [String] HTML
      def layout(name, assigns = {})
        @page_name = name
        content = render(name, assigns)

        render("layout", content: content)
      end

      # ----- escaping and links -----------------------------------------

      # @param value [Object] any value; nil becomes ""
      # @return [String] HTML-escaped text
      def h(value)
        ERB::Util.html_escape(value.to_s)
      end

      # Builds a link under the mount path. The result is not HTML-escaped.
      #
      # @param path [String] path that starts with "/"
      # @param params [Hash, nil] query values. nil values are dropped. Array
      #   values use "key[]".
      # @return [String]
      def url(path, params = nil)
        target = "#{root}#{path}"
        return target unless params

        pairs = params.compact.flat_map do |key, value|
          Array(value).map { |item| [value.is_a?(Array) ? "#{key}[]" : key.to_s, item.to_s] }
        end

        pairs.empty? ? target : "#{target}?#{URI.encode_www_form(pairs)}"
      end

      # @return [String] the request path relative to the mount path
      def current_path
        request.path
      end

      # @param prefix [String] navigation path; "/" matches only the overview
      # @return [Boolean] true when the request path is in that section
      def active?(prefix)
        (prefix == "/") ? request.path == "/" : request.path.start_with?(prefix)
      end

      # ----- formatting -------------------------------------------------

      # @param value [Time, nil] nil renders ""
      # @param relative [Boolean] show "5m ago" text instead of a UTC date
      # @return [String] an escaped <time> element with the exact UTC value in
      #   its title
      def time(value, relative: true)
        return "" unless value

        iso = value.utc.iso8601
        label = relative ? relative_time(value) : value.utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        %(<time datetime="#{h(iso)}" title="#{h(value.utc.strftime("%Y-%m-%d %H:%M:%S.%3N UTC"))}">#{h(label)}</time>)
      end

      # @param value [Time]
      # @param now [Time] reference time
      # @return [String] for example "now", "3m ago", or "in 2h 5m". Not
      #   escaped.
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

      # @param seconds [Numeric, nil] duration in seconds; nil renders ""
      # @return [String] for example "<1 ms", "250 ms", or "2m 3s"
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

      # @param value [#to_i]
      # @return [String] the integer with comma separators
      def number(value)
        value.to_i.to_s.reverse.scan(/\d{1,3}/).join(",").reverse
      end

      # @param status [String] a derived status from {Queries::STATUSES}
      # @return [String] display label; unknown values are returned as is
      def status_label(status)
        STATUS_LABELS.fetch(status.to_s, status.to_s)
      end

      # @param status [String] a derived status
      # @return [String] an escaped badge element
      def status_badge(status)
        %(<span class="status status-#{h(status)}">#{h(status_label(status))}</span>)
      end

      # @param id [String] UUID
      # @return [String] the first 8 characters
      def short_id(id)
        id.to_s[0, 8]
      end

      # @param value [Object]
      # @return [String] indented JSON, or #inspect when JSON fails. Not
      #   escaped.
      def pretty_json(value)
        JSON.pretty_generate(value)
      rescue JSON::GeneratorError
        value.inspect
      end

      # @param text [#to_s]
      # @param length [Integer] maximum length, including the ellipsis
      # @return [String] not escaped
      def truncate(text, length = 80)
        text = text.to_s
        (text.length > length) ? "#{text[0, length - 1]}…" : text
      end

      # Hash keys that Active Job and {Arguments} add to serialized arguments.
      INTERNAL_KEYS = [Arguments::RUBY2_KEYWORDS_KEY, Arguments::SYMBOL_KEYS_KEY, Arguments::INDIFFERENT_KEY, Arguments::OBJECT_KEY].freeze

      # @param job [Hash{String => Object}] a decoded job row
      # @return [Array<Object>] the job arguments without serializer markers
      def arguments_of(job)
        clean_arguments(job.fetch("payload", {}).fetch("arguments", []))
      end

      # @param job [Hash{String => Object}] a decoded job row
      # @return [String] arguments as comma-separated JSON. Not escaped.
      def argument_preview(job)
        arguments_of(job).map { |argument| JSON.generate(argument) }.join(", ")
      rescue JSON::GeneratorError
        arguments_of(job).inspect
      end

      # Strips Active Job's serialization markers so operators see their own values.
      #
      # @param value [Object] serialized arguments
      # @return [Object] a copy without {INTERNAL_KEYS} at any depth
      def clean_arguments(value)
        case value
        when Hash then value.except(*INTERNAL_KEYS).to_h { |key, item| [key, clean_arguments(item)] }
        when Array then value.map { |item| clean_arguments(item) }
        else value
        end
      end

      # @param error [Hash{String => Object}, nil] a decoded error column
      # @return [String] "Class: message", or "" when there is no error. Not
      #   escaped.
      def error_summary(error)
        return "" unless error.is_a?(Hash)

        "#{error["class"]}: #{truncate(error["message"], 160)}"
      end

      # @param lines [Array<String>, nil] backtrace lines
      # @return [Array<String>] lines outside gems, Ruby, DecentJob, and
      #   Active Job
      def application_backtrace(lines)
        Array(lines).reject { |line| line.to_s.match?(%r{/gems/|/ruby/\d|<internal:|/lib/decent_job/|/lib/active_job/}) }
      end

      # ----- forms ------------------------------------------------------

      # @return [String] the hidden authenticity_token input that every POST
      #   form must include
      def csrf_field
        %(<input type="hidden" name="authenticity_token" value="#{h(csrf_token)}">)
      end

      # Renders a one-button POST form with the CSRF token.
      #
      # @param path [String] action path under the mount path
      # @param label [String] button text
      # @param params [Hash] extra hidden fields
      # @param confirm [String, nil] confirmation text for dashboard.js; nil
      #   for none
      # @param kind [String] button style class
      # @return [String] escaped HTML
      def action_button(path, label, params: {}, confirm: nil, kind: "quiet")
        hidden = params.map { |key, value| %(<input type="hidden" name="#{h(key)}" value="#{h(value)}">) }.join
        confirm_attribute = confirm ? %( data-confirm="#{h(confirm)}") : ""
        %(<form method="post" action="#{h(url(path))}" class="inline"#{confirm_attribute}>#{csrf_field}#{hidden}<button class="button #{h(kind)}">#{h(label)}</button></form>)
      end

      private

      # Compiles each template once per View, so once per request.
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
