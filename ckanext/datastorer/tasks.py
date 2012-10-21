import json
from messytables import (CSVTableSet, XLSTableSet, types_processor,
                         headers_guess, headers_processor, type_guess,
                         offset_processor)
from ckanext.archiver.tasks import download, update_task_status
from ckan.lib.celery_app import celery
import requests
import datetime
import dateutil.parser as parser
import messytables

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
    messytables.types.IntegerType: 'numeric', # 'int' may not be big enough, and type detection may not realize it needs to be big
    messytables.types.FloatType: 'float',
    messytables.types.DecimalType: 'numeric',
    messytables.types.DateType: 'timestamp',
    DateUtilType: 'timestamp'
}


class DatastoreError(Exception):
    pass


def check_response_and_retry(response, datastore_request_url):
    try:
        if not response.status_code:
            raise DatastoreError('Datastore is not reponding at %s with '
                                 'response %s' % (datastore_request_url,
                                                  response))
        if not response.status_code:
            raise DatastoreError('Datastore responded at %s with '
                                 'response %s' % (datastore_request_url,
                                                  response))
    except Exception, e:
        datastorer_upload.retry(exc=e)

def log_response(logger, message, resource, response):
    if response.status_code == 200:
        logger.info(message.format(res_id=resource['id'], response="Success"))
    else:
        err = response.content
        try:
            import pprint, json
            err = "\n" + pprint.pformat(json.loads(response.content)["error"], indent=4, width=40)
        except:
            pass
        logger.error(message.format(res_id=resource['id'], response="Error: Response Code " + str(response.status_code) + ": " + err))

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
    row_set.register_processor(datetime_procesor())

    guessed_types = type_guess(
        row_set.sample,
        [
            messytables.types.StringType,
            messytables.types.IntegerType,
            messytables.types.FloatType,
            messytables.types.DecimalType,
            DateUtilType
        ],
        strict=True
    )
    row_set.register_processor(types_processor(guessed_types))
    row_set.register_processor(stringify_processor())

    ckan_url = context['site_url'].rstrip('/')

    datastore_request_url = '%s/api/action/datastore_' % (ckan_url)

    guessed_type_names = [TYPE_MAPPING[type(gt)] for gt in guessed_types]

    def send_request(data, action, logmessage):
        request = {'resource_id': resource['id'] }
        if action == "create":
            request['fields'] = [dict(id=name, type=typename) for name, typename in zip(headers, guessed_type_names)]
            request['records'] = []
        if action == "upsert":
            request['records'] = data
            request['method'] = 'insert'
            
        #import pprint
        #print datastore_request_url+action
        #pprint.pprint(request)
        #print
            
        response = requests.post(datastore_request_url+action,
                             data=json.dumps(request),
                             headers={'Content-Type': 'application/json',
                                      'Authorization': context['apikey']},
                             )
        
        log_response(logger, logmessage, resource, response)
        
        return response

    send_request(None, "delete", "Deleted {res_id}: {response}.")
    send_request(None, "create", "Created {res_id}: {response}.")

    data = []
    count = 0
    for dict_ in row_set.dicts():
        data.append(dict(dict_))
        count += 1
        if len(data) == 100:
            response = send_request(data, "upsert", "Uploaded to {res_id}: {response}.")
            check_response_and_retry(response, datastore_request_url + "upsert")
            data[:] = []

    if len(data) > 0:
        response = send_request(data, "upsert", "Uploaded to {res_id}: {response}.")
        check_response_and_retry(response, datastore_request_url + "upsert")

    logger.info("There should be {n} entries in {res_id}.".format(n=count, res_id=resource['id']))

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
        raise DatastoreError('Ckan bad response code (%s). Response was %s' %
                             (response.status_code, response.content))
