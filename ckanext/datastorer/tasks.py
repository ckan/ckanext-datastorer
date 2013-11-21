import json
import requests
import datetime
import itertools

import locale

import messytables
from messytables import (any_tableset, types_processor,
                         headers_guess, headers_processor, headers_make_unique,
                         type_guess, offset_processor)
from ckanext.archiver.tasks import download, update_task_status
from ckan.lib.celery_app import celery
from common import DATA_FORMATS, TYPE_MAPPING

if not locale.getlocale()[0]:
    locale.setlocale(locale.LC_ALL, '')


class DatastorerException(Exception):
    pass


def get_response_error(response):
    if not response.content:
        return repr(response)
    try:
        d = json.loads(response.content)
    except ValueError:
        return repr(response) + " <" + response.content + ">"
    if "error" in d:
        d = d["error"]
    return repr(response) + "\n" + json.dumps(d, sort_keys=True, indent=4) + "\n"


def check_response_and_retry(response, datastore_create_request_url, logger):
    try:
        if not response.status_code:
            raise DatastorerException('Datastore is not reponding at %s with '
                    'response %s' % (datastore_create_request_url, response))
    except Exception, e:
        datastorer_upload.retry(exc=e)

    if response.status_code not in (201, 200):
        logger.error('Response was {0}'.format(get_response_error(response)))
        raise DatastorerException('Datastorer bad response code (%s) on %s. Response was %s' %
                (response.status_code, datastore_create_request_url, response))


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
            if isinstance(cell.value, datetime.datetime):
                cell.value = cell.value.isoformat()
                cell.type = messytables.StringType()
        return row
    return datetime_convert


@celery.task(name="datastorer.upload", max_retries=24 * 7,
             default_retry_delay=3600)
def datastorer_upload(context, data):
    logger = datastorer_upload.get_logger()
    try:
        data = json.loads(data)
        context = json.loads(context)
        return _datastorer_upload(context, data, logger)
    except Exception, e:
        update_task_status(context, {
            'entity_id': data['id'],
            'entity_type': u'resource',
            'task_type': 'datastorer',
            'key': u'celery_task_id',
            'value': unicode(datastorer_upload.request.id),
            'error': '%s: %s' % (e.__class__.__name__,  unicode(e)),
            'last_updated': datetime.datetime.now().isoformat()
        }, logger)
        raise


def _datastorer_upload(context, resource, logger):
    result = download(context, resource, data_formats=DATA_FORMATS)

    content_type = result['headers'].get('content-type', '')\
                                    .split(';', 1)[0]  # remove parameters

    f = open(result['saved_file'], 'rb')
    table_sets = any_tableset(f, mimetype=content_type, extension=resource['format'].lower())

    ##only first sheet in xls for time being
    row_set = table_sets.tables[0]
    offset, headers = headers_guess(row_set.sample)
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

    ckan_url = context['site_url'].rstrip('/')

    datastore_create_request_url = '%s/api/action/datastore_create' % (ckan_url)

    guessed_type_names = [TYPE_MAPPING[type(gt)] for gt in guessed_types]

    def send_request(data):
        request = {'resource_id': resource['id'],
                   'fields': [dict(id=name, type=typename) for name, typename in zip(headers, guessed_type_names)],
                   'force': True,
                   'records': data}
        response = requests.post(datastore_create_request_url,
                         data=json.dumps(request),
                         headers={'Content-Type': 'application/json',
                                  'Authorization': context['apikey']},
                         )
        check_response_and_retry(response, datastore_create_request_url, logger)

    # Delete any existing data before proceeding. Otherwise 'datastore_create' will
    # append to the existing datastore. And if the fields have significantly changed,
    # it may also fail.
    try:
        logger.info('Deleting existing datastore (it may not exist): {0}.'.format(resource['id']))
        response = requests.post('%s/api/action/datastore_delete' % (ckan_url),
                                 data=json.dumps({'resource_id': resource['id'], 'force': True}),
                        headers={'Content-Type': 'application/json',
                                'Authorization': context['apikey']}
                        )
        if not response.status_code or response.status_code not in (200, 404):
            # skips 200 (OK) or 404 (datastore does not exist, no need to delete it)
            logger.error('Deleting existing datastore failed: {0}'.format(get_response_error(response)))
            raise DatastorerException("Deleting existing datastore failed.")
    except requests.exceptions.RequestException as e:
        logger.error('Deleting existing datastore failed: {0}'.format(str(e)))
        raise DatastorerException("Deleting existing datastore failed.")

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
    for data in chunky(row_set.dicts(), 100):
        count += len(data)
        send_request(data)

    logger.info("There should be {n} entries in {res_id}.".format(n=count, res_id=resource['id']))

    ckan_request_url = ckan_url + '/api/action/resource_update'

    resource.update({
        'webstore_url': 'active',
        'webstore_last_updated': datetime.datetime.now().isoformat()
    })

    response = requests.post(
        ckan_request_url,
        data=json.dumps(resource),
        headers={'Content-Type': 'application/json',
                 'Authorization': context['apikey']})

    if response.status_code not in (201, 200):
        raise DatastorerException('Ckan bad response code (%s). Response was %s' %
                             (response.status_code, response.content))
