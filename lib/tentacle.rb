require 'tentacle/version'
require 'gooddata'
require 'tempfile'
require 'aws-sdk'

module Tentacle
  class Extractor

    include Methadone::CLILogging
    include Methadone::Main

    def initialize(pid,username,password,s3_key,s3_secret)
      @pid = pid
      @start_time = Time.now

      @dir = Dir.mktmpdir(@start_time.strftime('%Y%m%d-%H%M%S.%L'))
      puts @dir

      GoodData.connect(username, password)
      GoodData.use(pid)

      @s3 = AWS::S3.new(
        :access_key_id => s3_key,
        :secret_access_key => s3_secret
      )
    end

    def get_id_from_url(url)
      url.scan(/.*\/(\d+)$/).last.last.to_i
    end

    def run
      get_users
      ldm_result = get_ldm
      ref_integrity_fails = validate
      get_datasets(ldm_result,ref_integrity_fails)
      get_metrics
      get_reports
      get_dashboards

      puts 'Duration ' + (Time.now - @start_time).to_s + ' s'
    end

    def save_to_file(content, name, dir=nil)
      if dir
        unless File.directory?(@dir + '/' + dir)
          FileUtils.mkdir_p(@dir + '/' + dir)
        end
      end
      path = @dir + '/' + (dir ? dir + '/' : '') + name + '.json'
      f = File.new(path, 'w')
      f.write(content.to_json)
      f.close
    end

    def get_users
      users = GoodData.get(sprintf('/gdc/projects/%s/users', @pid))
      users['users'].each { |user|
        user_object_id = user['user']['links']['self'][21..-1]
        save_to_file(user, user_object_id, 'users')
      }
    end

    def get_ldm
      ldm_poll = GoodData.get(sprintf('/gdc/projects/%s/model/view', @pid))
      finished = false
      ldm_result = nil
      until finished
        ldm_result = GoodData.get(ldm_poll['asyncTask']['link']['poll'])
        finished = !(defined? ldm_result['asyncTask']['link']['poll'])
      end
      save_to_file(ldm_result, 'ldm', nil)
      ldm_result
    end

    def validate
      ref_integrity_fails = {}
      validations_by_objects = {}
      validation_poll = GoodData.post(sprintf('/gdc/md/%s/validate', @pid), { 'validateProject' => ['ldm','pdm','invalid_objects'] })
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

        ref_integrity_fails
      }

      validations_by_objects.each {|key, value|
        save_to_file(value, 'validation', key)
      }
    end

    def get_datasets(ldm_result,ref_integrity_fails)
      ldm_datasets = []
      ldm_dimensions = []

      datasets = GoodData.get(sprintf('/gdc/md/%s/data/sets', @pid))
      datasets['dataSetsInfo']['sets'].each { |dataset|
        dataset_detail = GoodData.get(dataset['meta']['uri'])
        dataset_object_id = get_id_from_url(dataset['meta']['uri'])

        dataset_folder = 'datasets/' + dataset_object_id.to_s
        save_to_file(dataset_detail, 'detail', dataset_folder)
        save_to_file(GoodData.get(sprintf('/gdc/md/%s/usedby/%s', @pid, dataset_object_id.to_s)), 'used_by', dataset_folder)
        save_to_file(GoodData.get(sprintf('/gdc/md/%s/using/%s', @pid, dataset_object_id.to_s)), 'using', dataset_folder)

        dataset_detail['dataSet']['content']['attributes'].each { |attribute_url|
          attribute = GoodData.get(attribute_url)
          attribute_object_id = get_id_from_url(attribute_url)
          attribute_folder = dataset_folder + '/attributes/' + attribute_object_id.to_s

          save_to_file(attribute, 'detail', attribute_folder)
          save_to_file(GoodData.get(sprintf('/gdc/md/%s/usedby/%s', @pid, attribute_object_id.to_s)), 'used_by', attribute_folder)
          save_to_file(GoodData.get(sprintf('/gdc/md/%s/using/%s', @pid, attribute_object_id.to_s)),  'using', attribute_folder)
        }


        dataset_detail['dataSet']['content']['facts'].each { |fact_url|
          fact = GoodData.get(fact_url)
          fact_object_id = get_id_from_url(fact_url)
          fact_folder = dataset_folder + '/facts/' + fact_object_id.to_s

          save_to_file(fact, 'detail', fact_folder)
          save_to_file(GoodData.get(sprintf('/gdc/md/%s/usedby/%s', @pid, fact_object_id.to_s)), 'used_by', fact_folder)
          save_to_file(GoodData.get(sprintf('/gdc/md/%s/using/%s', @pid, fact_object_id.to_s)), 'using', fact_folder)
        }


        uploads_list = GoodData.get(dataset['dataUploads'])
        uploads_list['dataUploads']['uploads'].each { |upload|
          upload_object_id = get_id_from_url(upload['dataUpload']['uri'])
          save_to_file(upload, upload_object_id.to_s, dataset_folder + '/uploads')
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

        metric_folder = 'metrics/' + metric_object_id.to_s
        save_to_file(metric_detail, 'detail', metric_folder)
        save_to_file(GoodData.get(sprintf('/gdc/md/%s/usedby/%s', @pid, metric_object_id.to_s)), 'used_by', metric_folder)
        save_to_file(GoodData.get(sprintf('/gdc/md/%s/using/%s', @pid, metric_object_id.to_s)), 'using', metric_folder)
      }
    end

    def get_reports
      GoodData::Report[:all].each { |report|
        report_detail = GoodData.get(report['link'])
        report_object_id = get_id_from_url(report['link'])

        report_folder = 'reports/' + report_object_id.to_s
        save_to_file(report_detail, 'detail', report_folder)

        report_detail['report']['content']['definitions'].each { |definition_url|
          definition_detail = GoodData.get(definition_url)
          definition_object_id = get_id_from_url(definition_url)
          definition_folder = report_folder + '/definitions/' + definition_object_id.to_s

          save_to_file(definition_detail, 'detail', definition_folder)
          save_to_file(GoodData.get('/gdc/md/' + @pid + '/usedby/' + definition_object_id.to_s), 'used_by', definition_folder)
          save_to_file(GoodData.get('/gdc/md/' + @pid + '/using/' + definition_object_id.to_s), 'using', definition_folder)
        }

        save_to_file(GoodData.get(sprintf('/gdc/md/%s/usedby/%s', @pid, report_object_id.to_s)), 'used_by', report_folder)
        save_to_file(GoodData.get(sprintf('/gdc/md/%s/using/%s', @pid, report_object_id.to_s)), 'using', report_folder)
      }
    end

    def get_dashboards
      dashboards = GoodData.get('/gdc/md/' + @pid + '/query/projectdashboards')
      dashboards['query']['entries'].each { |dashboard|
        dashboard_object_id = get_id_from_url(dashboard['link'])

        dashboard_folder = 'dashboards/' + dashboard_object_id.to_s
        save_to_file(GoodData.get(dashboard['link']), 'detail', dashboard_folder)
        save_to_file(GoodData.get('/gdc/md/' + @pid + '/usedby/' + dashboard_object_id.to_s), 'used_by', dashboard_folder)
        save_to_file(GoodData.get('/gdc/md/' + @pid + '/using/' + dashboard_object_id.to_s), 'using', dashboard_folder)
      }
    end

  end

end
