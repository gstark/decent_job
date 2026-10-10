# frozen_string_literal: true

module RuboCop
  module Cop
    module DecentJob
      # Separates multi-line control structures and blocks from the statements
      # around them with one blank line.
      #
      # A statement counts when it is, or ends with, a multi-line `if`,
      # `unless`, `case`, `while`, `until`, `for`, `begin`, or block. The first
      # and last statements of a body are exempt; the Layout/EmptyLinesAround*
      # cops own those edges.
      #
      # @example
      #   # bad
      #   timeout = configuration.timeout
      #   unless timeout.positive?
      #     errors.add(:timeout, "must be positive")
      #   end
      #   validate_queues
      #
      #   # good
      #   timeout = configuration.timeout
      #
      #   unless timeout.positive?
      #     errors.add(:timeout, "must be positive")
      #   end
      #
      #   validate_queues
      class EmptyLinesAroundMultilineStatement < Base
        include StatementSpacing
        extend AutoCorrector

        MSG_BEFORE = "Add an empty line before this multi-line statement."
        MSG_AFTER = "Add an empty line after this multi-line statement."
        COMPOUND_TYPES = %i[if case case_match while until while_post until_post for block numblock itblock kwbegin].freeze

        def on_begin(node)
          node.children.each_cons(2) do |prev, curr|
            next if curr.first_line <= last_line(prev)
            next if blank_line_between?(prev, curr)

            if compound?(curr)
              add_offense(line_range(curr.first_line), message: MSG_BEFORE) { |corrector| insert_blank_line_after(corrector, prev) }
            elsif compound?(prev)
              add_offense(line_range(last_line(prev)), message: MSG_AFTER) { |corrector| insert_blank_line_after(corrector, prev) }
            end
          end
        end
        alias_method :on_kwbegin, :on_begin

        private

        # The statement is, or ends with, a multi-line control structure or block.
        def compound?(node)
          return false unless node.multiline?

          [node, *node.each_descendant(*COMPOUND_TYPES)].any? do |candidate|
            COMPOUND_TYPES.include?(candidate.type) && candidate.multiline? && candidate.last_line == node.last_line && keyword_form?(candidate)
          end
        end

        # A wrapped modifier (`foo(a,\n b) if x`) or a wrapped ternary reads as a call, not as a control structure.
        def keyword_form?(node)
          return true unless node.respond_to?(:modifier_form?)

          !node.modifier_form? && !(node.if_type? && node.ternary?)
        end
      end
    end
  end
end
