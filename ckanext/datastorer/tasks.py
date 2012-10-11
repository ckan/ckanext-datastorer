import json
import itertools
import requests
import datetime

import messytables
from messytables import (CSVTableSet, XLSTableSet, types_processor,
                         headers_guess, headers_processor, type_guess,
                         offset_processor)
from ckanext.archiver.tasks import download, update_task_status
from ckan.lib.celery_app import celery
import dateutil.parser as parser

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


class DateUtilType(messytables.types.CellType):
    """ The date util type uses the dateutil library to
    parse the dates."""
    guessing_weight = 5

    def cast(self, value):
        return parser.parse(value)


TYPE_MAPPING = {
    messytables.types.StringType: 'text',
    messytables.types.IntegerType: 'int',
    messytables.types.FloatType: 'float',
    messytables.types.DecimalType: 'numeric',
    messytables.types.DateType: 'timestamp',
    DateUtilType: 'timestamp'
}


class DatastorerException(Exception):
    pass


def check_response_and_retry(response, datastore_request_url):
    try:
        if not response.status_code:
            raise DatastorerException('Datastore is not reponding at %s with '
                    'response %s' % (datastore_request_url, response))
    except Exception, e:
        datastorer_upload.retry(exc=e)

    if response.status_code not in (201, 200):
        raise DatastorerException('Datastorer bad response code (%s) on %s. Response was %s' %
                (response.status_code, datastore_request_url, response))


def stringify_processor():
    def to_string(row_set, row):
        for cell in row:
            if not cell.value:
                cell.value = None
            else:
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

    excel_types = ['xls', 'application/ms-excel', 'application/xls',
                   'application/vnd.ms-excel']
    tsv_types = ['tsv', 'text/tsv', 'text/tab-separated-values']

    result = download(context, resource, data_formats=DATA_FORMATS)

    content_type = result['headers'].get('content-type', '')\
                                    .split(';', 1)[0]  # remove parameters

    f = open(result['saved_file'], 'rb')

    is_excel = False

    if content_type in excel_types or resource['format'] in excel_types:
        table_sets = XLSTableSet.from_fileobj(f)
        is_excel = True
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
    row_set.register_processor(datetime_procesor())

    logger.info('Header offset: {0}.'.format(offset))
    print "offset", offset

    guessed_types = type_guess(
        itertools.islice(row_set.sample, offset + 1, None),
        [
            messytables.types.StringType,
            messytables.types.IntegerType,
            messytables.types.FloatType,
            messytables.types.DecimalType,
            DateUtilType
        ],
        strict=True
    )
    #row_set.register_processor(offset_processor(offset + 1))
    row_set.register_processor(types_processor(guessed_types))
    row_set.register_processor(stringify_processor())

    ckan_url = context['site_url'].rstrip('/')

    datastore_request_url = '%s/api/action/datastore_create' % (ckan_url)

    guessed_type_names = [TYPE_MAPPING[type(gt)] for gt in guessed_types]

    def send_request(data):
        request = {'resource_id': resource['id'],
                   'fields': [dict(id=name, type=typename) for name, typename in zip(headers, guessed_type_names)],
                   'records': data}

        return requests.post(datastore_request_url,
                             data=json.dumps(request),
                             headers={'Content-Type': 'application/json',
                                      'Authorization': context['apikey']},
                             )

    data = []
    count = 0

    dicts = row_set.dicts()
    if is_excel:
        dicts = itertools.islice(dicts, offset + 1, None)
    for count, dict_ in enumerate(dicts):
        data.append(dict(dict_))
        if (count % 100) == 0:
            response = send_request(data)
            check_response_and_retry(response, datastore_request_url)
            data[:] = []

    if data:
        response = send_request(data)
        check_response_and_retry(response, datastore_request_url + '_mapping')

    logger.info("There should be {n} entries in {res_id}.".format(n=count + 1, res_id=resource['id']))

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
        raise DatastorerException('Ckan bad response code (%s). Response was %s' %
                             (response.status_code, response.content))
