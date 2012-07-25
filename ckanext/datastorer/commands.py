from datetime import datetime
import json
import requests
import urlparse
from pylons import config
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action
from ckan import model
from ckan.model.types import make_uuid
import logging
logger = logging.getLogger()


class Webstorer(CkanCommand):
    """
    Upload all available resources to the webstore

    Usage:

        paster datastorer update
           - Archive all resources or just those belonging to a specific
             package if a package id is provided

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
        #import after load config so CKAN_CONFIG evironment variable can be set
        from ckan.lib.celery_app import celery
        import tasks
        user = get_action('get_site_user')({'model': model,
                                            'ignore_auth': True}, {})
        context = json.dumps({
            'site_url': config['ckan.site_url'],
            'apikey': user.get('apikey'),
            'username': user.get('name'),
            'webstore_url': config.get('ckan.webstore_url')
        })
        api_url = urlparse.urljoin(config['ckan.site_url'], 'api/action')

        if cmd == 'update':
            response = requests.post(api_url +
                                     '/current_package_list_with_resources',
                                     "{}")
            packages = json.loads(response.content).get('result')

            for package in packages:
                for resource in package.get('resources', []):
                    data = json.dumps(resource, {'model': model})

                    if resource['webstore_url']:
                        continue
                    mimetype = resource['mimetype']
                    if mimetype and (mimetype not in tasks.DATA_FORMATS
                                     or resource['format'] not in
                                     tasks.DATA_FORMATS):
                        continue

                    logger.info('Webstoring resource from resource %s from '
                                'package %s' % (resource['url'],
                                                package['name']))

                    task_id = make_uuid()
                    datastorer_task_status = {
                        'entity_id': resource['id'],
                        'entity_type': u'resource',
                        'task_type': u'datastorer',
                        'key': u'celery_task_id',
                        'value': task_id,
                        'last_updated': datetime.now().isoformat()
                    }
                    datastorer_task_context = {
                        'model': model,
                        'user': user.get('name')
                    }

                    get_action('task_status_update')(datastorer_task_context,
                                                     datastorer_task_status)
                    celery.send_task("datastorer.upload",
                                     args=[context, data],
                                     task_id=task_id)
        else:
            logger.error('Command %s not recognized' % (cmd,))
