# frozen_string_literal: true

module DecentJob
  # The few core extensions the engine needs, without patching core classes.
  #
  # The core must run without Active Support, so these helpers replace
  # `constantize`, `deep_dup`, `blank?`, `class_attribute`, and `pluralize`.
  # All methods are stateless module functions and are safe on any thread.
  module Support
    module_function

    # Resolves a constant from its name.
    #
    # const_get triggers autoload and const_missing, so host autoloaders work.
    #
    # @param name [String, Symbol] constant path, with or without a leading "::"
    # @return [Object] the constant
    # @raise [NameError] when the constant does not exist
    # @note Can load application code through the host autoloader.
    def constantize(name)
      Object.const_get(name.to_s.delete_prefix("::"))
    end

    # Copies nested hashes, arrays, and strings.
    #
    # Other objects keep their identity. Hash default values and default procs
    # are not copied: the result is always a plain Hash.
    #
    # @param value [Object] the value to copy
    # @return [Object] the copy
    def deep_dup(value)
      case value
      when Hash then value.each_with_object({}) { |(key, item), copy| copy[deep_dup(key)] = deep_dup(item) }
      when Array then value.map { |item| deep_dup(item) }
      when String then value.dup
      else value
      end
    end

    # Tells whether a value is blank, with Active Support semantics.
    #
    # Honor an object's own blank? (ActiveSupport defines it everywhere).
    #
    # @param value [Object] the value to test
    # @return [Boolean] true for nil, false, whitespace-only strings, and empty
    #   collections
    def blank?(value)
      return value.blank? if value.respond_to?(:blank?)
      return true if value.nil? || value == false
      return value.match?(/\A[[:space:]]*\z/) if value.is_a?(String)

      value.respond_to?(:empty?) ? value.empty? : false
    end

    # @param value [Object] the value to test
    # @return [Boolean] the inverse of {.blank?}
    def present?(value)
      !blank?(value)
    end

    # Defines an inheritable class-level setting.
    #
    # A class-level setting that subclasses inherit until they assign their
    # own value, like ActiveSupport's class_attribute without instance access.
    # A subclass that assigns a value shadows its parent from then on. Mutating
    # an inherited value in place changes it for the parent too, so callers
    # assign a new object instead.
    #
    # @param klass [Class] the class that gets the reader and writer
    # @param name [Symbol] the setting name
    # @param default [Object] the value when no class in the chain assigned one
    # @return [Symbol] the name of the defined writer
    def class_setting(klass, name, default)
      variable = :"@#{name}"

      klass.define_singleton_method(name) do
        if instance_variable_defined?(variable)
          instance_variable_get(variable)
        elsif superclass.respond_to?(name)
          superclass.public_send(name)
        else
          default
        end
      end

      klass.define_singleton_method(:"#{name}=") { |value| instance_variable_set(variable, value) }
    end

    # Picks the singular or plural word for a count.
    #
    # @param count [Integer] the count
    # @param singular [String] the word for a count of one
    # @param plural [String] the word for other counts
    # @return [String] the chosen word, without the count
    def pluralize(count, singular, plural = "#{singular}s")
      (count == 1) ? singular : plural
    end
  end
end
