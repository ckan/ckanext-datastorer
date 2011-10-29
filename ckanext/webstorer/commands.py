from datetime import datetime
import json
import requests
import urlparse
from pylons import config
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action
from ckan import model
import tasks
import logging
logger = logging.getLogger()

class Webstorer(CkanCommand):
    """
    Upload all available resources to the webstore

    Usage:

        paster webstorer update 
           - Archive all resources or just those belonging to a specific package 
             if a package id is provided

    """    
    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 1 
    max_args = 1 

    def command(self):
        """
        Parse command line arguments and call appropriate method.
        """
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print Webstorer.__doc__
            return

        cmd = self.args[0]
        self._load_config()
        user = get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
        context = json.dumps({
            'site_url': config['ckan.site_url'],
            'apikey': user.get('apikey'),
            'username': user.get('name'),
            'webstore_url': config.get('ckan.webstore_url')
        })
        api_url = urlparse.urljoin(config['ckan.site_url'], 'api/action')

        if cmd == 'update':
            response = requests.post(api_url + '/current_package_list_with_resources', "{}")
            packages = json.loads(response.content).get('result')

            for package in packages:
                for resource in package.get('resources', []):
                    data = json.dumps(resource, {'model': model})
                    
                    if resource['webstore_url']:
                        continue
                    mimetype = resource['mimetype']
                    if mimetype and (mimetype not in tasks.DATA_FORMATS 
                                     or resource['format'] not in tasks.DATA_FORMATS):
                        continue

                    logger.info('Webstoring resource from resource %s from package %s' % (
                        resource['url'], package['name']))
                    webstorer_task = tasks.webstorer_upload.delay(context, data) 
                    # update the task_status table
                    webstorer_task_status = {
                        'entity_id': resource['id'],
                        'entity_type': u'resource',
                        'task_type': u'archiver',
                        'key': u'celery_task_id',
                        'value': webstorer_task.task_id,
                        'last_updated': datetime.now().isoformat()
                    }
                    webstorer_task_context = {
                        'model': model, 
                        'session': model.Session, 
                        'user': user.get('name')
                    }
                    get_action('task_status_update')(webstorer_task_context, 
                                                     webstorer_task_status)
        else:
            logger.error('Command %s not recognized' % (cmd,))

