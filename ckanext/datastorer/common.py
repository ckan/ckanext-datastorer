import messytables

DATA_FORMATS = [
    'csv',
    'tsv',
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
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'text/comma-separated-values',
    'application/x-zip-compressed',
    'application/zip',
]


TYPE_MAPPING = {
    messytables.types.StringType: 'text',
    # 'int' may not be big enough,
    # and type detection may not realize it needs to be big
    messytables.types.IntegerType: 'numeric',
    messytables.types.FloatType: 'float',
    messytables.types.DecimalType: 'numeric',
    messytables.types.DateType: 'timestamp',
    messytables.types.DateUtilType: 'timestamp'
}
