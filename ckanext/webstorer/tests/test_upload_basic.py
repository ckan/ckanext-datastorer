import ckanext.webstorer.tasks as tasks
from ckanext.archiver.tasks import LinkCheckerError
from nose.tools import assert_raises
import os
import json
from nose.tools import raises
import subprocess
import requests
import sqlite3
import time
import shutil
import uuid

class TestUploadBasic(object):

    @classmethod
    def setup_class(cls):

        fake_ckan_path = os.path.join(os.path.dirname(__file__), "fake_ckan.py")
        cls.fake_ckan = subprocess.Popen(['python', fake_ckan_path])
        
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
        cls.fake_ckan.kill()

    def make_resource_id(self):

        response = requests.post(
            'http://0.0.0.0:8088/api/action/package_create',
             data=json.dumps(
                 {'name': str(uuid.uuid4()), 
                  'resources': [{u'url': u'moo'}]}
             ),
             headers={'Authorization': 'moo'} 
        )

        return json.loads(response.content)['result']['resources'][0]['id']


    def test_csv_file(self):

        data = {'url': 'http://0.0.0.0:50001/static/simple.csv',
                'format': 'csv',
                'id': 'uuid1'}

        context = {'site_url': 'http://0.0.0.0:8088',
                   'apikey': 'moo',
                   'username': 'moo'}

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.webstorer_upload(json.dumps(context), json.dumps(data))

        import time; time.sleep(0.5)
        
        response = requests.get(
            'http://0.0.0.0:8088/api/data/%s/_search?q=*' % resource_id,
             )
    
        response = json.loads(response.content)
        
        assert len(response['hits']['hits']) == 6, len(response['hits']['hits'])


        
        
    def test_excel_file(self):
        
        data = {'url': 'http://0.0.0.0:50001/static/simple.xls',
                'format': 'xls',
                'id': 'uuid2'}
        context = {'site_url': 'http://0.0.0.0:8088',
                   'apikey': 'moo',
                   'username': 'moo'}
        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.webstorer_upload(json.dumps(context), json.dumps(data))
        response = requests.get('http://0.0.0.0:50002/test/uuid2/data.json')

        import time; time.sleep(0.5)

        response = requests.get(
            'http://0.0.0.0:8088/api/data/%s/_search?q=*' % resource_id,
             )
    
        response = json.loads(response.content)
        
        assert len(response['hits']['hits']) == 6, len(response['hits']['hits'])

    def test_messier_file(self):

        data = {'url': 'http://0.0.0.0:50001/static/3ffdcd42-5c63-4089-84dd-c23876259973',
                'format': 'csv'}
        
        context = {'site_url': 'http://0.0.0.0:8088',
                   'apikey': 'moo',
                   'username': 'moo'}

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        tasks.webstorer_upload(json.dumps(context), json.dumps(data))

        response = requests.get(
            'http://0.0.0.0:8088/api/data/%s/_search?q=*' % resource_id,
             )
    
        response = json.loads(response.content)
        
        assert len(response['hits']['hits']) == 10, len(response['hits']['hits'])

            
    def test_error_bad_url(self):

        data = {'url': 'http://0.0.0.0:50001/static/3ffdcd',
                'format': 'csv'}
        
        context = {'site_url': 'http://0.0.0.0:8088',
                   'apikey': 'moo',
                   'username': 'moo'}

        resource_id = self.make_resource_id()
        data['id'] = resource_id

        assert_raises(LinkCheckerError, tasks.webstorer_upload, json.dumps(context), json.dumps(data))



    #unicode error.
    #def test_error_bad_file(self):

        #data = {'url': 'http://0.0.0.0:50001/static/bad_file.csv',
                #'format': 'csv'}
        
        #context = {'site_url': 'http://0.0.0.0:8088',
                   #'apikey': 'moo',
                   #'username': 'moo'}

        #resource_id = self.make_resource_id()
        #data['id'] = resource_id

        #assert_raises(LinkCheckerError, tasks.webstorer_upload, json.dumps(context), json.dumps(data))

        #response = requests.get(
            #'http://0.0.0.0:8088/api/data/%s/_search?q=*' % resource_id,
             #)
    
        #response = json.loads(response.content)
        #assert response.status_code == 404

