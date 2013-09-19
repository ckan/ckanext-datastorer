import sys
from datetime import datetime
import itertools
import json
import messytables
from messytables import (any_tableset, types_processor, headers_guess,
                         headers_processor, type_guess, offset_processor)
import os
import requests
import urlparse
from pylons import config
from ckan.lib.cli import CkanCommand
from ckan.logic import get_action
from ckan import model
from ckan.model.types import make_uuid
import ckan.plugins.toolkit as toolkit
from common import DATA_FORMATS, TYPE_MAPPING
import fetch_resource
import logging
logger = logging.getLogger()


class DatastorerException(Exception):
    pass


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
    MAX_PER_PAGE = 50

    def _get_all_packages(self, api_url, headers):
        page = 1
        while True:
            response = requests.post(api_url +
                                     '/current_package_list_with_resources',
                                     '{"page": %d, "limit": %d}' %
                                     (page, self.MAX_PER_PAGE),
                                     headers=headers)
            packages = json.loads(response.content).get('result')
            if not packages:
                raise StopIteration
            for package in packages:
                yield package
            page += 1

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
                packages = self._get_all_packages(api_url, headers)

            for package in packages:
                for resource in package.get('resources', []):
                    data = json.dumps(resource, {'model': model})

                    # skip update if the datastore is already active (a table exists)
                    if resource.get('datastore_active'):
                        continue
                    mimetype = resource['mimetype']
                    if mimetype and not(mimetype in tasks.DATA_FORMATS or
                                        resource['format'].lower() in
                                        tasks.DATA_FORMATS):
                        logger.warn(u'Skipping resource %s from package %s '
                                u'because MIME type %s and format %s are '
                                u'unrecognized' % (resource['url'],
                                package['name'], mimetype, resource['format']))
                        continue

                    logger.info(u'Datastore resource from resource %s from '
                                u'package %s' % (resource['url'], package['name']))

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


class AddToDataStore(CkanCommand):
    """
    Upload all resources with a url and a mimetype/format matching allowed
    formats to the DataStore

    Usage:

    paster datastore_upload [package-id] -i/--ignore [package-id]
            - Update all resources.
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 0
    max_args = 1
    MAX_PER_PAGE = 50
    max_content_length = int(config.get('ckanext-archiver.max_content_length',
                             50000000))
    CkanCommand.parser.add_option('-i', '--ignore', dest="ignore",
                                  action="append",
                                  help="ID of a resource to ignore")
    CkanCommand.parser.add_option('--no-hash', dest="force",
                                  action="store_true",
                                  help="Do not check hashes")

    def _get_all_packages(self):
        page = 1
        context = {
            'model': model,
        }
        while True:
            data_dict = {
                'page': page,
                'limit': self.MAX_PER_PAGE,
            }
            packages = toolkit.get_action(
                'current_package_list_with_resources')(context, data_dict)
            if not packages:
                raise StopIteration
            for package in packages:
                yield package
            page += 1

    def command(self):
        """
        Parse command line arguments and call the appropriate method
        """
        if self.args and self.args[0] in ['--help', '-h', 'help']:
            print self.__doc__
            return

        self._load_config()
        user = toolkit.get_action('get_site_user')({'model': model,
                                                    'ignore_auth': True}, {})
        context = {'username': user.get('name'),
                   'user': user.get('name'),
                   'model': model}

        if len(self.args) == 1:
            data_dict = {'id': self.args[0]}
            try:
                packages = [
                    toolkit.get_action('package_show')(context, data_dict)
                ]
            except toolkit.ObjectNotFound:
                logger.error('Dataset %s not found' % self.args[0])
                sys.exit(1)
        else:
            packages = self._get_all_packages()

        resource_status = []
        for package in packages:
            for resource in package.get('resources', []):
                mimetype = resource['mimetype']
                if mimetype and not(mimetype in DATA_FORMATS or
                                    resource['format'].lower()
                                    in DATA_FORMATS):
                    logger.warn(u'Skipping resource {0} from package {1} '
                        u'because MIME type {2} and format {3} is '
                        u'unrecognized'.format(resource['url'],
                                              package['name'],
                                              mimetype,
                                              resource['format']))
                    continue
                if (self.options.ignore and resource['id'] in
                        self.options.ignore):
                    logger.warn('Ignoring resource {0}'.format(resource['id']))
                    continue
                logger.info(u'Datastore resource from resource {0} from '
                    u'package {0}'.format(resource['url'],
                                         package['name']))
                status = self.push_to_datastore(context, resource)
                if status['success'] is False:
                    resource_status.append(status)
        print resource_status

    def push_to_datastore(self, context, resource):
        try:
            original_hash = json.loads(resource.get('hash'))
            original_content_hash = original_hash['content']
            check_hash = not self.options.force
        except ValueError:
            original_content_hash = resource.get('hash')
            check_hash = False

        try:
            result = fetch_resource.download(context,
                                             resource,
                                             self.max_content_length,
                                             DATA_FORMATS,
                                             check_modified=check_hash)
        except fetch_resource.ResourceNotModified as e:
            logger.info(
                u'Skipping unmodified resource: {0}'.format(resource['url'])
            )
            return {'success': True,
                    'resource': resource['id'],
                    'error': None}
        except Exception as e:
            logger.exception(e)
            return {'success': False,
                    'resource': resource['id'],
                    'error': 'Could not download resource'}

        if check_hash and (result['hash'] == original_content_hash):
            logger.info(
                u'Skipping unmodified resource: {0}'.format(resource['url'])
            )
            os.remove(result['saved_file'])
            return {'success': True,
                    'resource': resource['id'],
                    'error': None}

        content_type = result['headers'].get('content-type', '')\
                                        .split(';', 1)[0]  # remove parameters

        f = open(result['saved_file'], 'rb')
        try:
            table_sets = any_tableset(
                f,
                mimetype=content_type,
                extension=resource['format'].lower()
            )
            # only first sheet in xls for time being
            row_set = table_sets.tables[0]
            offset, headers = headers_guess(row_set.sample)
        except Exception as e:
            logger.exception(e)
            os.remove(result['saved_file'])
            return {'success': False,
                    'resource': resource['id'],
                    'error': 'Error parsing the resource'}

        row_set.register_processor(headers_processor(headers))
        row_set.register_processor(offset_processor(offset + 1))
        row_set.register_processor(datetime_procesor())

        logger.info('Header offset: {0}.'.format(offset))

        guessed_types = type_guess(
            row_set.sample,
            [
                messytables.types.StringType,
                messytables.types.IntegerType,
                messytables.types.FloatType,
                messytables.types.DecimalType,
                messytables.types.DateUtilType
            ],
            strict=True
        )
        logger.info('Guessed types: {0}'.format(guessed_types))
        row_set.register_processor(types_processor(guessed_types, strict=True))
        row_set.register_processor(stringify_processor())

        guessed_type_names = [TYPE_MAPPING[type(gt)] for gt in
                              guessed_types]

        def send_request(data):
            data_dict = {
                'resource_id': resource['id'],
                'fields': [dict(id=name, type=typename) for name, typename
                           in zip(headers, guessed_type_names)],
                'records': data
            }
            response = toolkit.get_action('datastore_create')(
                context,
                data_dict
            )
            return response

        # Delete any existing data before proceeding. Otherwise
        # 'datastore_create' will append to the existing datastore. And if the
        # fields have significantly changed, it may also fail.
        logger.info('Trying to delete existing datastore for resource {0} '
                    '(may not exist).'.format(resource['id']))
        try:
            toolkit.get_action('datastore_delete')(
                context,
                {'resource_id': resource['id']}
            )
        except toolkit.ObjectNotFound:
            logger.info('Datastore not found for resource {0}.'.format(
                resource['id']))
        except Exception as e:
            logger.exception(e)

        logger.info('Creating: {0}.'.format(resource['id']))

        # generates chunks of data that can be loaded into ckan
        # n is the maximum size of a chunk
        def chunky(iterable, n):
            it = iter(iterable)
            while True:
                chunk = list(
                    itertools.imap(
                        dict, itertools.islice(it, n)))
                if not chunk:
                    return
                yield chunk

        count = 0
        try:
            for data in chunky(row_set.dicts(), 100):
                count += len(data)
                send_request(data)
        except Exception as e:
            logger.exception(e)
            os.remove(result['saved_file'])
            return {'success': False,
                    'resource': resource['id'],
                    'error': 'Error pushing data to datastore'}

        logger.info("There should be {n} entries in {res_id}.".format(
            n=count,
            res_id=resource['id']
        ))

        resource.update({
            'webstore_url': 'active',
            'webstore_last_updated': datetime.now().isoformat()
        })

        toolkit.get_action('resource_update')(context, resource)
        os.remove(result['saved_file'])
        return {'success': True,
                'resource': resource['id'],
                'error': None}


def stringify_processor():
    def to_string(row_set, row):
        for cell in row:
            if cell.value is None:
                continue
            else:
                cell.value = unicode(cell.value)
        return row
    return to_string


def datetime_procesor():
    ''' Stringifies dates so that they can be parsed by the db
    '''
    def datetime_convert(row_set, row):
        for cell in row:
            if isinstance(cell.value, datetime):
                cell.value = cell.value.isoformat()
                cell.type = messytables.StringType()
        return row
    return datetime_convert
