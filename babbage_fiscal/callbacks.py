import requests

STATUS_QUEUED = 'queued'
STATUS_INITIALIZING = 'initializing'
STATUS_LOADING_DATAPACKAGE = 'loading-datapackage'
STATUS_VALIDATING_DATAPACKAGE = 'validating-datapackage'
STATUS_LOADING_RESOURCE = 'loading-resource'
STATUS_DELETING_TABLE = 'deleting-table'
STATUS_CREATING_TABLE = 'creating-table'
STATUS_LOADING_DATA_READY = 'loading-data-ready'
STATUS_LOADING_DATA = 'loading-data'
STATUS_CREATING_BABBAGE_MODEL = 'creating-babbage-model'
STATUS_SAVING_METADATA = 'saving-metadata'
STATUS_DONE = 'done'
STATUS_FAIL = 'fail'


def do_request(callback, package, status, progress=None, error=None, data=None):
    params = {'package': package, 'status': status}
    if progress is not None:
        params['progress'] = progress
    if error is not None:
        params['error'] = error
    if data is not None:
        params['data'] = json.dumps(data, indent=0)
    requests.get(callback, params).content

