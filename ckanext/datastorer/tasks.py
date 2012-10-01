import json
from messytables import (CSVTableSet, XLSTableSet, types_processor,
                         headers_guess, headers_processor, type_guess,
                         offset_processor)
from ckanext.archiver.tasks import download, update_task_status
from ckan.lib.celery_app import celery
import requests
import datetime
import messytables

from logging import getLogger
logger = getLogger(__name__)

DATA_FORMATS = [
    'csv',
    'text/csv',
    'txt',
    'text/plain',
    'text/tsv',
    'text/tab-separated-values',
    'xls',
    'application/ms-excel',
    'application/vnd.ms-excel',
    'application/xls',
    'application/octet-stream',
    'text/comma-separated-values'
]


class WebstorerError(Exception):
    pass


def check_response_and_retry(response, webstore_request_url):
    try:
        if not response.status_code:
            raise WebstorerError('Webstore is not reponding at %s with '
                                 'response %s' % (webstore_request_url,
                                                  response))
    except Exception, e:
        datastorer_upload.retry(exc=e)


def stringify_processor():
    def to_string(row_set, row):
        for cell in row:
            cell.value = unicode(cell.value)
            cell.type = messytables.StringType()
        return row
    return to_string


def datetime_procesor():
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
    try:
        data = json.loads(data)
        context = json.loads(context)
        return _datastorer_upload(context, data)
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


def _datastorer_upload(context, resource):

    excel_types = ['xls', 'application/ms-excel', 'application/xls',
                   'application/vnd.ms-excel']
    tsv_types = ['tsv', 'text/tsv', 'text/tab-separated-values']

    result = download(context, resource, data_formats=DATA_FORMATS)

    content_type = result['headers'].get('content-type', '')\
                                    .split(';', 1)[0]  # remove parameters

    f = open(result['saved_file'], 'rb')

    if content_type in excel_types or resource['format'] in excel_types:
        table_sets = XLSTableSet.from_fileobj(f)
    else:
        is_tsv = (content_type in tsv_types or
                  resource['format'] in tsv_types)
        delimiter = '\t' if is_tsv else ','
        table_sets = CSVTableSet.from_fileobj(f, delimiter=delimiter)

    ##only first sheet in xls for time being
    row_set = table_sets.tables[0]
    offset, headers = headers_guess(row_set.sample)
    row_set.register_processor(headers_processor(headers))
    row_set.register_processor(offset_processor(offset + 1))
    #row_set.register_processor(datetime_procesor())

    guessed_types = type_guess(row_set)
    row_set.register_processor(offset_processor(offset + 1))
    row_set.register_processor(types_processor(guessed_types))
    row_set.register_processor(stringify_processor())

    ckan_url = context['site_url'].rstrip('/')

    webstore_request_url = '%s/api/action/datastore_create' % (ckan_url)

    def send_request(data):
        request = {'resource_id': resource['id'],
                   'fields': [dict(id=name) for name in headers],
                   'records': data}

        return requests.post(webstore_request_url,
                             data=json.dumps(request),
                             headers={'Content-Type': 'application/json',
                                      'Authorization': context['apikey']},
                             )

    data = []
    for count, dict_ in enumerate(row_set.dicts()):
        data.append(dict(dict_))
        if (count % 100) == 0:
            response = send_request(data)
            check_response_and_retry(response, webstore_request_url)
            data[:] = []

    if data:
        response = send_request(data)
        check_response_and_retry(response, webstore_request_url + '_mapping')

    ckan_request_url = ckan_url + '/api/action/resource_update'

    ckan_resource_data = {
        'id': resource["id"],
        'webstore_url': 'active',
        'webstore_last_updated': datetime.datetime.now().isoformat(),
        'url': resource['url']
    }

    response = requests.post(
        ckan_request_url,
        data=json.dumps(ckan_resource_data),
        headers={'Content-Type': 'application/json',
                 'Authorization': context['apikey']})

    if response.status_code not in (201, 200):
        raise WebstorerError('Ckan bad response code (%s). Response was %s' %
                             (response.status_code, response.content))
