# frozen_string_literal: true

module RuboCop
  module Cop
    module DecentJob
      # Shared helpers for cops that insert blank lines between sibling statements.
      module StatementSpacing
        private

        def blank_line_between?(prev, curr)
          processed_source.lines[last_line(prev)...(curr.first_line - 1)].any? { |line| line.strip.empty? }
        end

        # The last source line of a statement, including any heredoc bodies.
        def last_line(node)
          heredoc_ends = node.each_node(:str, :dstr, :xstr).select(&:heredoc?).map { |heredoc| heredoc.loc.heredoc_end.last_line }
          [node.last_line, *heredoc_ends].max
        end

        def line_range(line)
          processed_source.buffer.line_range(line)
        end

        # Insert directly after `prev` so a comment above the next statement stays attached to it.
        def insert_blank_line_after(corrector, prev)
          position = line_range(last_line(prev) + 1).begin_pos
          corrector.insert_before(Parser::Source::Range.new(processed_source.buffer, position, position), "\n")
        end
      end
    end
  end
end
