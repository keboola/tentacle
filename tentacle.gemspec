# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'tentacle/version'

Gem::Specification.new do |spec|
  spec.name          = "tentacle"
  spec.version       = Tentacle::VERSION
  spec.authors       = ["Jakub Matejka"]
  spec.email         = ["jakub@keboola.com"]
  spec.summary       = "Keboola Tentacle | versioning tool for GoodData projects"
  spec.description   = ""
  spec.homepage      = "http://tentacle.keboola.com"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler', '~> 1.6'
  spec.add_development_dependency 'rdoc'
  spec.add_development_dependency 'aruba'
  spec.add_development_dependency 'rake', '~> 0.9.2'
  spec.add_dependency 'methadone', '~> 1.5.0'
  spec.add_dependency 'gooddata'
  spec.add_dependency 'aws-sdk'
  spec.add_dependency 'newrelic_rpm'
end
