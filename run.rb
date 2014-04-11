require "gooddata"
require 'optparse'

options = {}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: run.rb [options]"
  opts.on('-p', '--pid PID', 'GoodData project id') { |v| options[:pid] = v }
  opts.on('-u', '--username USERNAME', 'GoodData user login') { |v| options[:username] = v }
  opts.on('-s', '--password PASSWORD', 'GoodData user password') { |v| options[:password] = v }
end.parse!

begin
  optparse.parse!
  mandatory = [:pid, :username, :password]
  missing = mandatory.select{ |param| options[param].nil? }
  unless missing.empty?
    puts "Missing options: #{missing.join(', ')}"
    puts optparse
    exit
  end
rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  puts $!.to_s
  puts optparse
  exit
end


start_time = Time.now
path_prefix = 'data/' + options[:pid] + '/' + start_time.strftime('%Y%m%d-%H%M%S.%L')

def get_id_from_url(url)
	url.scan(/.*\/(\d+)$/).last.last.to_i
end

def create_folder(path)
  unless File.directory?(path)
    FileUtils.mkdir_p(path)
	end
end

def save_to_file(object, file_name)
  file = File.new(file_name, 'w')
  file.puts(object.to_json)
  file.close
end

GoodData.connect(options[:username], options[:password])
GoodData.use(options[:pid])


puts '- ldm'
ldm_poll = GoodData.get('/gdc/projects/' + options[:pid] + '/model/view')
finished = false
ldm_result = nil
ldm_datasets = []
ldm_dimensions = []
until finished
  ldm_result = GoodData.get(ldm_poll['asyncTask']['link']['poll'])
  finished = !(defined? ldm_result['asyncTask']['link']['poll'])
end


puts '- validation'
create_folder(path_prefix + '/validation')
validations_by_objects = {}
validation_poll = GoodData.post('/gdc/md/' +  options[:pid] + '/validate', { 'validateProject' => ['ldm','pdm','invalid_objects'] })
finished = false
validation_result = nil
until finished
  validation_result = GoodData.get(validation_poll['asyncTask']['link']['poll'])
  finished = validation_result.has_key?('projectValidateResult')
end

validation_result['projectValidateResult']['results'].each { |validation_group|
  validation_group['body']['log'].each { |validation|
    params = []
    objects = []
    validation['pars'].each { |param|
      key, value = param.first
      case key
        when 'common'
          params << value
        when 'object'
          params << value['name']
          objects << value['id']
        when 'sli_el'
          params << value['vals'].to_s
        else
          params << value.to_s
          puts '!! UNKNOWN VALIDATION PARAM TYPE !!'
          puts key
          puts value
      end
    }

    message = sprintf(validation['msg'], *params)
    objects.each { |object|
      unless validations_by_objects.has_key?(object)
        validations_by_objects[object] = []
      end
      validations_by_objects[object] << { 'message' => message, 'level' => validation['level'], 'ecat' => validation['ecat'] }
    }
  }
}


validations_by_objects.each {|key, value|
  save_to_file(value, path_prefix + '/validation/' + key + '.json')
}



puts '- datasets'
create_folder(path_prefix + '/datasets')
datasets = GoodData.get('/gdc/md/' + options[:pid] + '/data/sets')
datasets['dataSetsInfo']['sets'].each { |dataset|
  dataset_detail = GoodData.get(dataset['meta']['uri'])
  dataset_object_id = get_id_from_url(dataset['meta']['uri'])
  create_folder(path_prefix + '/datasets/' + dataset_object_id.to_s)

  save_to_file(dataset_detail, path_prefix + '/datasets/' + dataset_object_id.to_s + '/definition.json')
  save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/usedby/' + dataset_object_id.to_s),
               path_prefix + '/datasets/' + dataset_object_id.to_s + '/used_by.json')
  save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/using/' + dataset_object_id.to_s),
               path_prefix + '/datasets/' + dataset_object_id.to_s + '/using.json')

  create_folder(path_prefix + '/datasets/' + dataset_object_id.to_s + '/attributes')
  dataset_detail['dataSet']['content']['attributes'].each { |attribute_url|
    attribute = GoodData.get(attribute_url)
    attribute_object_id = get_id_from_url(attribute_url)

    save_to_file(attribute, path_prefix + '/datasets/' + dataset_object_id.to_s + '/attributes/' + attribute_object_id.to_s + '.json')
    save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/usedby/' + attribute_object_id.to_s),
                 path_prefix + '/datasets/' + dataset_object_id.to_s + '/attributes/' + attribute_object_id.to_s + '.used_by.json')
    save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/using/' + attribute_object_id.to_s),
                 path_prefix + '/datasets/' + dataset_object_id.to_s + '/attributes/' + attribute_object_id.to_s + '.using.json')
  }


  create_folder(path_prefix + '/datasets/' + dataset_object_id.to_s + '/facts')
  dataset_detail['dataSet']['content']['facts'].each { |fact_url|
    fact = GoodData.get(fact_url)
    fact_object_id = get_id_from_url(fact_url)

    save_to_file(fact, path_prefix + '/datasets/' + dataset_object_id.to_s + '/facts/' + fact_object_id.to_s + '.json')
  }

  ldm_result['projectModelView']['model']['projectModel']['datasets'].each { |ldm_dataset|
    if ldm_dataset['dataset']['identifier'] == dataset_detail['dataSet']['meta']['identifier']
      ldm_dataset['dataset']['object_id'] = dataset_object_id
      ldm_datasets << ldm_dataset
    end
  }

  ldm_result['projectModelView']['model']['projectModel']['dateDimensions'].each { |ldm_dimension|
    if ldm_dimension['dateDimension']['name'] + '.dataset.dt' == dataset_detail['dataSet']['meta']['identifier']
      ldm_dimension['dateDimension']['object_id'] = dataset_object_id
      ldm_dimensions << ldm_dimension
    end
  }


  create_folder(path_prefix + '/datasets/' + dataset_object_id.to_s + '/uploads')
  uploads_list = GoodData.get(dataset['dataUploads'])
  uploads_list['dataUploads']['uploads'].each { |upload|
    upload_object_id = get_id_from_url(upload['dataUpload']['uri'])
    save_to_file(upload, path_prefix + '/datasets/' + dataset_object_id.to_s + '/uploads/' + upload_object_id.to_s + '.json')
  }

  puts ' - ' + dataset_detail['dataSet']['meta']['identifier']
}

save_to_file(ldm_result, path_prefix + '/ldm.json')


#TODO
exit


puts '- users'
create_folder(path_prefix + '/users')
users = GoodData.get('/gdc/projects/' + options[:pid] + '/users')
users['users'].each { |user|
  user_object_id = user['user']['links']['self'][21..-1]

  save_to_file(user, path_prefix + '/users/' + user_object_id.to_s + '.json')
}


puts '- metrics'
create_folder(path_prefix + '/metrics')
GoodData::Metric[:all].each { |metric|
	metric_detail = GoodData.get(metric['link'])
  metric_object_id = get_id_from_url(metric['link'])

  save_to_file(metric_detail, path_prefix + '/metrics/' + metric_object_id.to_s + '.json')
  save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/usedby/' + metric_object_id.to_s),
               path_prefix + '/metrics/' + metric_object_id.to_s + '.used_by.json')
  save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/using/' + metric_object_id.to_s),
               path_prefix + '/metrics/' + metric_object_id.to_s + '.using.json')
}


puts '- reports'
create_folder(path_prefix + '/reports')
create_folder(path_prefix + '/report-definitions')
GoodData::Report[:all].each { |report|
	report_detail = GoodData.get(report['link'])
	report_object_id = get_id_from_url(report['link'])

  save_to_file(report_detail, path_prefix + '/reports/' + report_object_id.to_s + '.json')

  report_detail['report']['content']['definitions'].each { |definition_url|
    definition_detail = GoodData.get(definition_url)
    definition_object_id = get_id_from_url(definition_url)

    save_to_file(definition_detail, path_prefix + '/report-definitions/' + definition_object_id.to_s + '.json')
    save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/usedby/' + definition_object_id.to_s),
                 path_prefix + '/report-definitions/' + definition_object_id.to_s + '.used_by.json')
    save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/using/' + definition_object_id.to_s),
                 path_prefix + '/report-definitions/' + definition_object_id.to_s + '.using.json')
  }

  save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/usedby/' + report_object_id.to_s),
               path_prefix + '/reports/' + report_object_id.to_s + '.used_by.json')
  save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/using/' + report_object_id.to_s),
               path_prefix + '/reports/' + report_object_id.to_s + '.using.json')
}


puts '- dashboards'
create_folder(path_prefix + '/dashboards')
dashboards = GoodData.get('/gdc/md/' + options[:pid] + '/query/projectdashboards')
dashboards['query']['entries'].each { |dashboard|
	dashboard_object_id = get_id_from_url(dashboard['link'])

  save_to_file(GoodData.get(dashboard['link']),
               path_prefix + '/dashboards/' + dashboard_object_id.to_s + '.json')
  save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/usedby/' + dashboard_object_id.to_s),
               path_prefix + '/dashboards/' + dashboard_object_id.to_s + '.used_by.json')
  save_to_file(GoodData.get('/gdc/md/' + options[:pid] + '/using/' + dashboard_object_id.to_s),
               path_prefix + '/dashboards/' + dashboard_object_id.to_s + '.using.json')
}

puts 'Duration ' + (Time.now - start_time).to_s + ' s'