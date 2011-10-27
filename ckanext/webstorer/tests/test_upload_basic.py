import ckanext.webstorer.tasks as tasks
import os
import json
import subprocess
import requests
import sqlite3
import time
import shutil

class TestUploadBasic(object):

    @classmethod
    def setup_class(cls):
        try:
            os.mkdir('/tmp/test_webstore')
        except OSError:
            # just incase it did not get deleted properly lasttime
            shutil.rmtree('/tmp/test_webstore')
            os.mkdir('/tmp/test_webstore')
        conn = sqlite3.connect('/tmp/fake_ckan.db')
        conn.execute('drop table if exists "user"')
        conn.execute('create table "user" (name text, apikey text)')
        conn.execute('insert into user values ("test", "test")')
        conn.commit()

        fake_ckan_path = os.path.join(os.path.dirname(__file__), "fake_ckan.py")
        cls.fake_ckan = subprocess.Popen(['python', fake_ckan_path])
        
        cls.webstore = subprocess.Popen(
            ['python', '-m', 'webstore.web'],
            env=dict(WEBSTORE_SETTINGS=os.path.join(os.path.dirname(__file__), "webstore_config.cfg"),
                     PATH=os.environ['PATH']
                    )
        )
        #make sure services are running
        for i in range(0,12):
            time.sleep(0.1)
            response1 = requests.get('http://0.0.0.0:50001')
            response2 = requests.get('http://0.0.0.0:50002')
            if not response1 or not response2:
                continue
            return
        raise Exception('services did not start!')


    @classmethod
    def teardown_class(cls):
        cls.fake_ckan.kill()
        cls.webstore.kill()
        shutil.rmtree('/tmp/test_webstore')

    def test_csv_file(self):

        file_path = os.path.join(os.path.dirname(__file__), "simple.csv")
        data = {'file_path': file_path,
                'resource_id': 'uuid1'}
        context = {'webstore_url': 'http://0.0.0.0:50002',
                   'site_url': 'http://0.0.0.0:50001',
                   'apikey': 'test',
                   'username': 'test'}
        tasks.webstorer_upload(json.dumps(context), json.dumps(data))

        response = requests.get('http://0.0.0.0:50002/test/uuid1/data.json')


        assert json.loads(response.content) == [{u'date': u'2011-01-01', u'place': u'Galway', u'__id__': 1, u'temperature': u'1'},
                                                {u'date': u'2011-01-02', u'place': u'Galway', u'__id__': 2, u'temperature': u'-1'},
                                                {u'date': u'2011-01-03', u'place': u'Galway', u'__id__': 3, u'temperature': u'0'},
                                                {u'date': u'2011-01-01', u'place': u'Berkeley', u'__id__': 4, u'temperature': u'6'},
                                                {u'date': u'2011-01-02', u'place': u'Berkeley', u'__id__': 5, u'temperature': u'8'},
                                                {u'date': u'2011-01-03', u'place': u'Berkeley', u'__id__': 6, u'temperature': u'5'}], json.loads(response.content)

        response = requests.get('http://0.0.0.0:50001/last_request')
        
        assert json.loads(response.content)['headers'] == {u'Content-Length': u'126',
                                                          u'Accept-Encoding': u'gzip',
                                                          u'Connection': u'close',
                                                          u'User-Agent': u'python-requests.org',
                                                          u'Host': u'0.0.0.0:50001',
                                                          u'Content-Type': u'application/json',
                                                          u'Authorization': u'test'}
        
        assert json.loads(response.content)['data']['id'] == 'uuid1'
        assert json.loads(response.content)['data']['webstore_url'] == 'http://0.0.0.0:50002/test/uuid1/data'
        
        
    def test_excel_file(self):

        file_path = os.path.join(os.path.dirname(__file__), "simple.xls")
        data = {'file_path': file_path,
                'resource_id': 'uuid2'}
        context = {'webstore_url': 'http://0.0.0.0:50002',
                   'site_url': 'http://0.0.0.0:50001',
                   'apikey': 'test',
                   'username': 'test'}
        tasks.webstorer_upload(json.dumps(context), json.dumps(data))

        response = requests.get('http://0.0.0.0:50002/test/uuid2/data.json')


        assert json.loads(response.content) == [{u'date': u'2011-01-01 00:00:00', u'place': u'Galway', u'__id__': 1, u'temperature': u'1.0'},
                                                {u'date': u'2011-01-02 00:00:00', u'place': u'Galway', u'__id__': 2, u'temperature': u'-1.0'},
                                                {u'date': u'2011-01-03 00:00:00', u'place': u'Galway', u'__id__': 3, u'temperature': u'0.0'},
                                                {u'date': u'2011-01-01 00:00:00', u'place': u'Berkeley', u'__id__': 4, u'temperature': u'6.0'},
                                                {u'date': u'2011-01-02 00:00:00', u'place': u'Berkeley', u'__id__': 5, u'temperature': u'8.0'},
                                                {u'date': u'2011-01-03 00:00:00', u'place': u'Berkeley', u'__id__': 6, u'temperature': u'5.0'}], json.loads(response.content)

        response = requests.get('http://0.0.0.0:50001/last_request')
        
        assert json.loads(response.content)['headers'] == {u'Content-Length': u'126',
                                                          u'Accept-Encoding': u'gzip',
                                                          u'Connection': u'close',
                                                          u'User-Agent': u'python-requests.org',
                                                          u'Host': u'0.0.0.0:50001',
                                                          u'Content-Type': u'application/json',
                                                          u'Authorization': u'test'}
        
        assert json.loads(response.content)['data']['id'] == 'uuid2'
        assert json.loads(response.content)['data']['webstore_url'] == 'http://0.0.0.0:50002/test/uuid2/data'

    def test_messier_file(self):

        file_path = os.path.join(os.path.dirname(__file__), "3ffdcd42-5c63-4089-84dd-c23876259973")
        
        data = {'file_path': file_path,
                'resource_id': 'uuid3'}
        context = {'webstore_url': 'http://0.0.0.0:50002',
                   'site_url': 'http://0.0.0.0:50001',
                   'apikey': 'test',
                   'username': 'test'}
        tasks.webstorer_upload(json.dumps(context), json.dumps(data))

        response = requests.get('http://0.0.0.0:50002/test/uuid3/data.json')


        assert json.loads(response.content)[:3] == [
            {u'Date': u'01/04/2009', u'Transaction Number': u'136980', u'Amount': u'2840.5000', u'Expense Area': u'HOUSING HEALTH + COMMUNITY SAFETY',
             u'__id__': 1, u'Supplier': u'B H HAYES + SONS', u'Body Name': u'Adur District Council'},
            {u'Date': u'01/04/2009', u'Transaction Number': u'139471', u'Amount': u'997.8100', u'Expense Area': u'STRATEGIC PERFORMANCE,HR&TRANSFORMATION',
             u'__id__': 2, u'Supplier': u'BADENOCH + CLARK', u'Body Name': u'Adur District Council'},
            {u'Date': u'01/04/2009', u'Transaction Number': u'139723', u'Amount': u'356.4000', u'Expense Area': u'RECYCLING & WASTE DIVISION',
             u'__id__': 3, u'Supplier': u'B-O-S RECRUITMENT SERVICES', u'Body Name': u'Adur District Council'}], json.loads(response.content)[:3]









