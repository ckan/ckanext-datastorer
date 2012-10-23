import sys
from datetime import datetime
import json
import requests
import urlparse
from pylons import config
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action
from ckan import model
from ckan.model.types import make_uuid
import tasks
import logging
logger = logging.getLogger()


class Datastorer(CkanCommand):
    """
    Upload a resource or all resources in the datastore.

    Usage:

    paster datastorer [update|queue] [package-id]
           - Update all resources or just those belonging to a specific
             package if a package id is provided. Use 'update' to update
             the resource synchronously and log to stdout, or 'queue'
             to queue the update to run asynchronously in celery
             (output goes to celery's logs).

    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 1
    max_args = 2

    def command(self):
        """
        Parse command line arguments and call appropriate method.
        """
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print Datastorer.__doc__
            return

        cmd = self.args[0]
        self._load_config()
        #import after load config so CKAN_CONFIG evironment variable can be set
        from ckan.lib.celery_app import celery
        import tasks
        user = get_action('get_site_user')({'model': model,
                                            'ignore_auth': True}, {})
        context = {
            'site_url': config['ckan.site_url'],
            'apikey': user.get('apikey'),
            'site_user_apikey': user.get('apikey'),
            'username': user.get('name'),
            'webstore_url': config.get('ckan.webstore_url')
        }
        if not config['ckan.site_url']:
            raise Exception('You have to set the "ckan.site_url" property in your ini file.')
        api_url = urlparse.urljoin(config['ckan.site_url'], 'api/action')

        if cmd in ('update', 'queue'):
            headers = {
                'content-type:': 'application/json'
            }

            if len(self.args) == 2:
                response = requests.post(api_url +
                                         '/package_show',
                                         json.dumps({"id": self.args[1]}), headers=headers)
                if response.status_code == 200:
                    packages = [json.loads(response.content).get('result')]
                elif response.status_code == 404:
                    logger.error('Dataset %s not found' % self.args[1])
                    sys.exit(1)
                else:
                    logger.error('Error getting dataset %s' % self.args[1])
                    sys.exit(1)

            else:
                response = requests.post(api_url +
                                         '/current_package_list_with_resources',
                                         "{}", headers=headers)
                packages = json.loads(response.content).get('result')

            for package in packages:
                for resource in package.get('resources', []):
                    data = json.dumps(resource, {'model': model})

                    # skip update if the datastore is already active (a table exists)
                    if resource.get('datastore_active'):
                        continue
                    mimetype = resource['mimetype']
                    if mimetype and (mimetype not in tasks.DATA_FORMATS
                                     or resource['format'].lower() not in
                                     tasks.DATA_FORMATS):
                        logger.warn('Skipping resource %s from package %s because MIME type %s or format %s is unrecognized'
                              % (resource['url'], package['name'], mimetype, resource['format']))
                        continue

                    logger.info('Datastore resource from resource %s from '
                                'package %s' % (resource['url'],
                                                package['name']))

                    if cmd == "update":
                        logger.setLevel(0)
                        tasks._datastorer_upload(context, resource, logger)
                    elif cmd == "queue":
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
                                     args=[json.dumps(context), data],
                                     task_id=task_id)
        else:
            logger.error('Command %s not recognized' % (cmd,))
