source "https://rubygems.org"

gemspec

group :development, :test do
  gem "activejob", "~> 8.1", ">= 8.1.4"
  gem "minitest", "~> 5.27"
  gem "rake", "~> 13.4"
end

# Standard's dependencies need Ruby 3.3. Ruby 3.2 installs skip this group.
group :lint do
  gem "standard", "~> 1.56"
end
