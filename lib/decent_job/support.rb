# frozen_string_literal: true

module DecentJob
  # The few core extensions the engine needs, without patching core classes.
  module Support
    module_function

    # const_get triggers autoload and const_missing, so host autoloaders work.
    def constantize(name)
      Object.const_get(name.to_s.delete_prefix("::"))
    end

    def deep_dup(value)
      case value
      when Hash then value.each_with_object({}) { |(key, item), copy| copy[deep_dup(key)] = deep_dup(item) }
      when Array then value.map { |item| deep_dup(item) }
      when String then value.dup
      else value
      end
    end

    # Honor an object's own blank? (ActiveSupport defines it everywhere).
    def blank?(value)
      return value.blank? if value.respond_to?(:blank?)
      return true if value.nil? || value == false
      return value.match?(/\A[[:space:]]*\z/) if value.is_a?(String)

      value.respond_to?(:empty?) ? value.empty? : false
    end

    def present?(value)
      !blank?(value)
    end

    def pluralize(count, singular, plural = "#{singular}s")
      (count == 1) ? singular : plural
    end
  end
end
