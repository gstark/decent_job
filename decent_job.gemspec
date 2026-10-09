# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "decent_job"
  spec.version = "0.1.0"
  spec.authors = ["DecentJob contributors"]
  spec.summary = "A PostgreSQL Active Job backend with minimal dependencies"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2"
  spec.files = Dir["lib/**/*", "exe/*", "README.md", "LICENSE", "db/**/*", "docs/**/*"]
  spec.bindir = "exe"
  spec.executables = ["decent_job"]
  spec.require_paths = ["lib"]
  spec.add_dependency "activejob", "~> 8.1", ">= 8.1.4"
  spec.add_dependency "pg", "~> 1.7"
end
