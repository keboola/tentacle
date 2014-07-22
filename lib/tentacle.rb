require 'tentacle/version'
require 'gooddata'
require 'tempfile'
require 'elasticsearch'

module Tentacle
  class Extractor

    include Methadone::CLILogging
    include Methadone::Main

    def initialize(pid,username,password)
      @pid = pid
      @start_time = Time.now
      @path_prefix = 'data/' + pid + '/' + @start_time.strftime('%Y%m%d-%H%M%S.%L')

      GoodData.connect(username, password)
      GoodData.use(pid)

      #client = Elasticsearch::Client.new log: true
      #client.transport.reload_connections!
      #client.cluster.health
      #client.search q: 'test'
    end

    def get_id_from_url(url)
      url.scan(/.*\/(\d+)$/).last.last.to_i
    end

    def run
      get_users
      #get_ldm
      #validate
      #get_datasets
      #get_metrics
      #get_reports
      #get_dashboards

      puts 'Duration ' + (Time.now - @start_time).to_s + ' s'
    end

    def save_to_file(content, name)
      file = Tempfile.new(name)
      file.puts(content)
      file.close
      file.path
    end

    def get_users
      file = Tempfile.new('users')

      users = GoodData.get('/gdc/projects/' + @pid + '/users')
      users['users'].each { |user|
        user_object_id = user['user']['links']['self'][21..-1]
        file.puts(user)
      }

      file_path = file.path
      file.close

      File.open(file_path, 'r') do |f|
        f.each_line do |line|
          puts line
        end
      end
    end

    def get_ldm
      ldm_poll = GoodData.get(sprintf('/gdc/projects/%s/model/view', @pid))
      finished = false
      ldm_result = nil
      ldm_datasets = []
      ldm_dimensions = []
      until finished
        ldm_result = GoodData.get(ldm_poll['asyncTask']['link']['poll'])
        finished = !(defined? ldm_result['asyncTask']['link']['poll'])
      end

      file = Tempfile.new('ldm')
      file.write(ldm_result)

      puts file.path
      file.read
      file.close
    end

    def validate
      ref_integrity_fails = {}
      validations_by_objects = {}
      validation_poll = GoodData.post('/gdc/md/' +  @pid + '/validate', { 'validateProject' => ['ldm','pdm','invalid_objects'] })
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

          if validation['ecat'] == 'REF_INTEGRITY'
            source = validation['pars'][1]['object']['id']
            target = 'dataset.' + validation['pars'][5]['object']['name']

            unless ref_integrity_fails.has_key?(source)
              ref_integrity_fails[source] = []
            end
            ref_integrity_fails[source] << target
          end
        }
      }

      validations_by_objects.each {|key, value|
        puts key
        puts value

        file = Tempfile.new('validation-' + key)
        file.write(ldm_result)

        puts file.path
        file.read
        file.close
      }
    end

    def get_datasets
      datasets = GoodData.get('/gdc/md/' + @pid + '/data/sets')
      datasets['dataSetsInfo']['sets'].each { |dataset|
        dataset_detail = GoodData.get(dataset['meta']['uri'])
        dataset_object_id = get_id_from_url(dataset['meta']['uri'])

        save_to_file(dataset_detail, path_prefix + '/datasets/' + dataset_object_id.to_s + '/definition.json')
        save_to_file(GoodData.get('/gdc/md/' + @pid + '/usedby/' + dataset_object_id.to_s),
                     path_prefix + '/datasets/' + dataset_object_id.to_s + '/used_by.json')
        save_to_file(GoodData.get('/gdc/md/' + @pid + '/using/' + dataset_object_id.to_s),
                     path_prefix + '/datasets/' + dataset_object_id.to_s + '/using.json')

        dataset_detail['dataSet']['content']['attributes'].each { |attribute_url|
          attribute = GoodData.get(attribute_url)
          attribute_object_id = get_id_from_url(attribute_url)

          save_to_file(attribute, path_prefix + '/datasets/' + dataset_object_id.to_s + '/attributes/' + attribute_object_id.to_s + '.json')
          save_to_file(GoodData.get('/gdc/md/' + @pid + '/usedby/' + attribute_object_id.to_s),
                       path_prefix + '/datasets/' + dataset_object_id.to_s + '/attributes/' + attribute_object_id.to_s + '.used_by.json')
          save_to_file(GoodData.get('/gdc/md/' + @pid + '/using/' + attribute_object_id.to_s),
                       path_prefix + '/datasets/' + dataset_object_id.to_s + '/attributes/' + attribute_object_id.to_s + '.using.json')
        }


        dataset_detail['dataSet']['content']['facts'].each { |fact_url|
          fact = GoodData.get(fact_url)
          fact_object_id = get_id_from_url(fact_url)

          save_to_file(fact, path_prefix + '/datasets/' + dataset_object_id.to_s + '/facts/' + fact_object_id.to_s + '.json')
          save_to_file(GoodData.get('/gdc/md/' + @pid + '/usedby/' + fact_object_id.to_s),
                       path_prefix + '/datasets/' + dataset_object_id.to_s + '/facts/' + fact_object_id.to_s + '.used_by.json')
          save_to_file(GoodData.get('/gdc/md/' + @pid + '/using/' + fact_object_id.to_s),
                       path_prefix + '/datasets/' + dataset_object_id.to_s + '/facts/' + fact_object_id.to_s + '.using.json')
        }


        uploads_list = GoodData.get(dataset['dataUploads'])
        uploads_list['dataUploads']['uploads'].each { |upload|
          upload_object_id = get_id_from_url(upload['dataUpload']['uri'])
          save_to_file(upload, path_prefix + '/datasets/' + dataset_object_id.to_s + '/uploads/' + upload_object_id.to_s + '.json')
        }

        ldm_result['projectModelView']['model']['projectModel']['datasets'].each { |ldm_dataset|
          if ldm_dataset['dataset']['identifier'] == dataset_detail['dataSet']['meta']['identifier']
            ldm_dataset['dataset']['object_id'] = dataset_object_id

            if ref_integrity_fails.has_key?(dataset_object_id.to_s)
              ldm_dataset['dataset']['ref_integrity_fails'] = ref_integrity_fails[dataset_object_id.to_s]
            end
            ldm_datasets << ldm_dataset
          end
        }

        ldm_result['projectModelView']['model']['projectModel']['dateDimensions'].each { |ldm_dimension|
          if ldm_dimension['dateDimension']['name'] + '.dataset.dt' == dataset_detail['dataSet']['meta']['identifier']
            ldm_dimension['dateDimension']['object_id'] = dataset_object_id
            ldm_dimensions << ldm_dimension
          end
        }

        puts ' - ' + dataset_detail['dataSet']['meta']['identifier']
      }
    end

    def get_metrics
      GoodData::Metric[:all].each { |metric|
        metric_detail = GoodData.get(metric['link'])
        metric_object_id = get_id_from_url(metric['link'])

        save_to_file(metric_detail, path_prefix + '/metrics/' + metric_object_id.to_s + '.json')
        save_to_file(GoodData.get('/gdc/md/' + @pid + '/usedby/' + metric_object_id.to_s),
                     path_prefix + '/metrics/' + metric_object_id.to_s + '.used_by.json')
        save_to_file(GoodData.get('/gdc/md/' + @pid + '/using/' + metric_object_id.to_s),
                     path_prefix + '/metrics/' + metric_object_id.to_s + '.using.json')
      }
    end

    def get_reports
      GoodData::Report[:all].each { |report|
        report_detail = GoodData.get(report['link'])
        report_object_id = get_id_from_url(report['link'])

        save_to_file(report_detail, path_prefix + '/reports/' + report_object_id.to_s + '.json')

        report_detail['report']['content']['definitions'].each { |definition_url|
          definition_detail = GoodData.get(definition_url)
          definition_object_id = get_id_from_url(definition_url)

          save_to_file(definition_detail, path_prefix + '/report-definitions/' + definition_object_id.to_s + '.json')
          save_to_file(GoodData.get('/gdc/md/' + @pid + '/usedby/' + definition_object_id.to_s),
                       path_prefix + '/report-definitions/' + definition_object_id.to_s + '.used_by.json')
          save_to_file(GoodData.get('/gdc/md/' + @pid + '/using/' + definition_object_id.to_s),
                       path_prefix + '/report-definitions/' + definition_object_id.to_s + '.using.json')
        }

        save_to_file(GoodData.get('/gdc/md/' + @pid + '/usedby/' + report_object_id.to_s),
                     path_prefix + '/reports/' + report_object_id.to_s + '.used_by.json')
        save_to_file(GoodData.get('/gdc/md/' + @pid + '/using/' + report_object_id.to_s),
                     path_prefix + '/reports/' + report_object_id.to_s + '.using.json')
      }
    end

    def get_dashboards
      dashboards = GoodData.get('/gdc/md/' + @pid + '/query/projectdashboards')
      dashboards['query']['entries'].each { |dashboard|
        dashboard_object_id = get_id_from_url(dashboard['link'])

        save_to_file(GoodData.get(dashboard['link']),
                     path_prefix + '/dashboards/' + dashboard_object_id.to_s + '.json')
        save_to_file(GoodData.get('/gdc/md/' + @pid + '/usedby/' + dashboard_object_id.to_s),
                     path_prefix + '/dashboards/' + dashboard_object_id.to_s + '.used_by.json')
        save_to_file(GoodData.get('/gdc/md/' + @pid + '/using/' + dashboard_object_id.to_s),
                     path_prefix + '/dashboards/' + dashboard_object_id.to_s + '.using.json')
      }
    end

  end

end
