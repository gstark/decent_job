# frozen_string_literal: true

module RuboCop
  module Cop
    module DecentJob
      # Separates an explicit `return` from the statement before it with one
      # blank line.
      #
      # A `return` that is the only statement in its body is exempt. So is a
      # `return` that sits directly inside a conditional or loop body, where
      # the short branch reads as one unit.
      #
      # @example
      #   # bad
      #   def total
      #     sum = items.sum
      #     return sum
      #   end
      #
      #   # good
      #   def total
      #     sum = items.sum
      #
      #     return sum
      #   end
      #
      #   # good
      #   def total
      #     return items.sum
      #   end
      #
      #   # good
      #   unless valid?
      #     errors.add(:base, "is invalid")
      #     return
      #   end
      class EmptyLineBeforeReturn < Base
        include StatementSpacing
        extend AutoCorrector

        MSG = "Add an empty line before `return`."
        BRANCH_TYPES = %i[if case when case_match in_pattern while until while_post until_post for].freeze

        def on_begin(node)
          return if BRANCH_TYPES.include?(node.parent&.type)

          node.children.each_cons(2) do |prev, curr|
            next unless curr.return_type?
            next if curr.first_line <= last_line(prev)
            next if blank_line_between?(prev, curr)

            add_offense(curr) { |corrector| insert_blank_line_after(corrector, prev) }
          end
        end
        alias_method :on_kwbegin, :on_begin
      end
    end
  end
end
