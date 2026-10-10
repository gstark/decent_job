# frozen_string_literal: true

require "rake/testtask"

Rake::TestTask.new(:spec) do |task|
  task.libs << "spec"
  task.pattern = "spec/**/*_spec.rb"
end

task default: :spec

Rake::TestTask.new("spec:rails") do |task|
  task.libs << "spec"
  task.pattern = "integration/rails*_spec.rb"
end

Rake::TestTask.new("spec:pgbouncer") do |task|
  task.libs << "spec"
  task.pattern = "integration/pgbouncer/**/*_spec.rb"
end
