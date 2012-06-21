import ckanext.datastorer.tasks as tasks
from ckanext.archiver.tasks import LinkCheckerError
from nose.tools import assert_raises
import os
import json
import subprocess
import requests
import time
import uuid
import ConfigParser




class TestUploadBasic(object):

    @classmethod
    def setup_class(cls):

        # get config options
        config = ConfigParser.RawConfigParser({
            'proxy_host': '0.0.0.0',
        })
        config.read(os.path.join(os.path.dirname(os.path.abspath( __file__ )), 'tests_config.cfg'))

        cls.host = config.get('tests','proxy_host')
        cls.api_key = config.get('tests','user_api_key')

        if not cls.api_key:
            raise Exception('You must add a sysadmin API key to the tests configuration file')


        static_files_server = os.path.join(os.path.dirname(__file__), "static_files_server.py")
        cls.static_files_server = subprocess.Popen(['python', static_files_server])

        #make sure services are running
        for i in range(0,50):
            time.sleep(0.1)
            response1 = requests.get('http://0.0.0.0:50001')
            if not response1:
                continue
            return


        cls.teardown_class()
        raise Exception('services did not start!')

    @classmethod
    def teardown_class(cls):
        cls.static_files_server.kill()

    def make_resource_id(self):

        response = requests.post(
            'http://%s/api/action/package_create' % self.host,
             data=json.dumps(
                 {'name': str(uuid.uuid4()),
                  'resources': [{u'url': u'test'}]}
             ),
             headers={'Authorization': self.api_key}
        )
        return json.loads(response.content)['result']['resources'][0]['id']


    def test_csv_file(self):

        data = {'url': 'http://0.0.0.0:50001/static/simple.csv',
                'format': 'csv',
                'id': 'uuid1'}

        context = {'site_url': 'http://%s' % self.host,
                   'site_user_apikey': self.api_key,
                   'apikey': self.api_key
                  }

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.datastorer_upload(json.dumps(context), json.dumps(data))

        import time; time.sleep(0.5)

        response = requests.get(
            'http://%s/api/data/%s/_search?q=*' % (self.host,resource_id),
             )

        response = json.loads(response.content)

        assert len(response['hits']['hits']) == 6, len(response['hits']['hits'])




    def test_excel_file(self):

        data = {'url': 'http://0.0.0.0:50001/static/simple.xls',
                'format': 'xls',
                'id': 'uuid2'}
        context = {'site_url': 'http://%s' % self.host,
                   'apikey': self.api_key,
                   'site_user_apikey': self.api_key}

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.datastorer_upload(json.dumps(context), json.dumps(data))

        import time; time.sleep(0.5)

        response = requests.get(
            'http://%s/api/data/%s/_search?q=*' % (self.host,resource_id),
             )

        response = json.loads(response.content)

        assert len(response['hits']['hits']) == 6, len(response['hits']['hits'])

    def test_messier_file(self):

        data = {'url': 'http://0.0.0.0:50001/static/3ffdcd42-5c63-4089-84dd-c23876259973',
                'format': 'csv'}

        context = {'site_url': 'http://%s' % self.host,
                   'apikey': self.api_key,
                   'site_user_apikey': self.api_key,
                   }

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.datastorer_upload(json.dumps(context), json.dumps(data))

        response = requests.get(
            'http://%s/api/data/%s/_search?q=*' % (self.host,resource_id),
             )

        response = json.loads(response.content)

        assert len(response['hits']['hits']) == 10, len(response['hits']['hits'])


    def test_error_bad_url(self):

        data = {'url': 'http://0.0.0.0:50001/static/3ffdcd',
                'format': 'csv'}

        context = {'site_url': 'http://%s' % self.host,
                   'apikey': self.api_key,
                   'site_user_apikey': self.api_key,
                  }

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        assert_raises(LinkCheckerError, tasks.datastorer_upload, json.dumps(context), json.dumps(data))

