import ckanext.datastorer.tasks as tasks
from ckanext.archiver.tasks import LinkCheckerError
from nose.tools import assert_raises, assert_equal
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
            'ckan_host': '0.0.0.0',
        })
        config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 'tests_config.cfg'))

        cls.host = config.get('tests', 'ckan_host')
        cls.api_key = config.get('tests', 'user_api_key')

        if not cls.api_key:
            raise Exception('You must add a sysadmin API key to the tests '
                            ' configuration file')

        static_files_server = os.path.join(os.path.dirname(__file__),
                                           'static_files_server.py')
        cls.static_files_server = subprocess.Popen(
            ['python', static_files_server])

        #make sure services are running
        for i in range(0, 50):
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
            headers={'Authorization': self.api_key, 'content-type': 'application/json'}
        )
        return json.loads(response.content)['result']['resources'][0]['id']

    def test_csv_file(self):

        data = {'url': 'http://0.0.0.0:50001/static/simple.csv',
                'format': 'csv',
                'id': 'uuid1'}

        context = {'site_url': 'http://%s' % self.host,
                   'site_user_apikey': self.api_key,
                   'apikey': self.api_key}

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.datastorer_upload(json.dumps(context), json.dumps(data))

        response = requests.get(
            'http://%s/api/action/datastore_search?resource_id=%s' % (self.host, resource_id),
             headers={"content-type":"application/json"})

        result = json.loads(response.content) 

        assert result['result']['total'] == 6, result['total']
        assert result['result']['fields'] == [{u'type': u'int4', u'id': u'_id'},
                                              {u'type': u'timestamp', u'id': u'date'},
                                              {u'type': u'int4', u'id': u'temperature'},
                                              {u'type': u'text', u'id': u'place'}], result['fields']



    def test_tsv_file(self):

        data = {'url': 'http://0.0.0.0:50001/static/simple.tsv',
                'format': 'tsv',
                'id': 'uuid3'}

        context = {'site_url': 'http://%s' % self.host,
                   'site_user_apikey': self.api_key,
                   'apikey': self.api_key}

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.datastorer_upload(json.dumps(context), json.dumps(data))

        response = requests.get(
            'http://%s/api/action/datastore_search?resource_id=%s' % (self.host, resource_id),
             headers={"content-type":"application/json"})

        result = json.loads(response.content)
        assert result['result']['total'] == 6, result['total']
        assert result['result']['fields'] == [{u'type': u'int4', u'id': u'_id'},
                                              {u'type': u'timestamp', u'id': u'date'},
                                              {u'type': u'int4', u'id': u'temperature'},
                                              {u'type': u'text', u'id': u'place'}] , result['fields']

    def test_tsv_file_with_incorrect_mimetype(self):
        '''Not all servers are well-behaved, and provide the wrong mime type.

        Force the test server to provide the wrong Content-Type by changing
        the filename to have a .txt extension.  However, the owner of the
        resource knows it's a tsv file, and can set the format directly.
        '''

        data = {'url': 'http://0.0.0.0:50001/static/tsv_as_txt.txt',
                'format': 'tsv',
                'id': 'uuid4'}

        context = {'site_url': 'http://%s' % self.host,
                   'site_user_apikey': self.api_key,
                   'apikey': self.api_key}

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.datastorer_upload(json.dumps(context), json.dumps(data))

        response = requests.get(
            'http://%s/api/action/datastore_search?resource_id=%s' % (self.host, resource_id),
             headers={"content-type":"application/json"})

        result = json.loads(response.content)
        assert result['result']['total'] == 6, result['total']
        assert result['result']['fields'] == [{u'type': u'int4', u'id': u'_id'},
                                              {u'type': u'timestamp', u'id': u'date'},
                                              {u'type': u'int4', u'id': u'temperature'},
                                              {u'type': u'text', u'id': u'place'}] , result['fields']

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

        response = requests.get(
            'http://%s/api/action/datastore_search?resource_id=%s' % (self.host, resource_id),
             headers={"content-type":"application/json"})

        result = json.loads(response.content)
        assert result['result']['total'] == 6, result['total']
        assert result['result']['fields'] == [{u'type': u'int4', u'id': u'_id'},
                                              {u'type': u'timestamp', u'id': u'date'},
                                              {u'type': u'int4', u'id': u'temperature'},
                                              {u'type': u'text', u'id': u'place'}], result['fields']

    def test_messier_file(self):

        data = {
            'url': 'http://0.0.0.0:50001/static/3ffdcd42-5c63-4089-84dd-c23876259973',
            'format': 'csv'}

        context = {'site_url': 'http://%s' % self.host,
                   'apikey': self.api_key,
                   'site_user_apikey': self.api_key,
                   }

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.datastorer_upload(json.dumps(context), json.dumps(data))

        response = requests.get(
            'http://%s/api/action/datastore_search?resource_id=%s' % (self.host, resource_id),
             headers={"content-type": "application/json"})

        result = json.loads(response.content)

        # TODO: this fails
        assert result['result']['total'] == 564, result['result']['total']
        assert len(result['result']['records']) == 100

        assert result['result']['fields'] == [{u'type': u'int4', u'id': u'_id'},
                                              {u'type': u'text', u'id': u'Body Name'},
                                              {u'type': u'timestamp', u'id': u'Date'},
                                              {u'type': u'int4', u'id': u'Transaction Number'},
                                              {u'type': u'float8', u'id': u'Amount'},
                                              {u'type': u'text', u'id': u'Supplier'},
                                              {u'type': u'text', u'id': u'Expense Area'}], result['result']['fields']

    def test_error_bad_url(self):

        data = {'url': 'http://0.0.0.0:50001/static/3ffdcd',
                'format': 'csv'}

        context = {'site_url': 'http://%s' % self.host,
                   'apikey': self.api_key,
                   'site_user_apikey': self.api_key}

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        assert_raises(LinkCheckerError,
                      tasks.datastorer_upload,
                      json.dumps(context),
                      json.dumps(data))
