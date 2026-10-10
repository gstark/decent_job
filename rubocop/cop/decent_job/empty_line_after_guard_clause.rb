# frozen_string_literal: true

module RuboCop
  module Cop
    module DecentJob
      # Separates a modifier guard clause from the statement after it with one
      # blank line.
      #
      # This is RuboCop's Layout/EmptyLineAfterGuardClause under a project
      # name. Standard disables the Layout cop and refuses to re-enable it
      # through extend_config, but it accepts a cop it does not configure.
      #
      # A guard is `return`, `raise`, `fail`, `break`, or `next` with an `if`
      # or `unless` modifier. Consecutive guards stay together. A guard that
      # is followed by `end`, `else`, `elsif`, `rescue`, or `ensure` needs
      # no blank line.
      #
      # @example
      #   # bad
      #   def take(pattern)
      #     match = pattern.match(@source, @position)
      #     return unless match
      #     @position = match.end(0)
      #   end
      #
      #   # good
      #   def take(pattern)
      #     match = pattern.match(@source, @position)
      #     return unless match
      #
      #     @position = match.end(0)
      #   end
      #
      #   # good
      #   return [] if parts.empty?
      #   return parts.first if parts.one?
      class EmptyLineAfterGuardClause < Layout::EmptyLineAfterGuardClause
      end
    end
  end
end
