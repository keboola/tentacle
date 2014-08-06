require 'methadone'
require 'minitest/autorun'
require 'tentacle.rb'

require 'gooddata'
require 'yaml'

class TestExtractor < MiniTest::Unit::TestCase

  def setup
    config = YAML.load_file('test/config.yml')

    GoodData.connect(config[:username], config[:password])
    GoodData::Model::ProjectBuilder.create('test_project') do |p|
      p.add_date_dimension('Product Date')

      p.add_dataset('categories') do |d|
        d.add_anchor('id')
        d.add_label('id', :reference => 'id')
        d.add_attribute('name')
      end

      p.add_dataset('products') do |d|
        d.add_anchor('id')
        d.add_label('id', :reference => 'id')
        d.add_attribute('name')
        d.add_fact('price')
        d.add_date('date', :dataset => 'Product Date')
        d.add_reference('category_id', :dataset => 'categories', :reference => 'id')
      end
    end

    puts GoodData.project.pid
    exit

    @extractor = Tentacle::Extractor.new(config[:pid], config[:username], config[:password], config[:s3_key], config[:s3_secret])
    @extractor.create_dir
  end

  def test_file_saving
    @extractor.get_users

  end
end
