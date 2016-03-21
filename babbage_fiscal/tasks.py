import json

from celery import Celery
import requests

from .config import get_engine, _set_connection_string
from .loader import FDPLoader

app = Celery('fdp_loader')
app.config_from_object('babbage_fiscal.celeryconfig')


def do_request(callback, package, status, progress=None, error=None, data=None):
    params = {'package': package, 'status': status}
    if progress is not None:
        params['progress'] = progress
    if error is not None:
        params['error'] = error
    if data is not None:
        params['data'] = json.dumps(data, indent=0)
    requests.get(callback, params).content

@app.task
def load_fdp_task(package, callback, connection_string=None):
    def send_progress(status='progress', count=0, data=None):
        do_request(callback, package, status, count, data)
    if connection_string is not None:
        _set_connection_string(connection_string)
    try:
        print("Starting to load %s" % package)
        send_progress(status='starting')
        model_name, package_contents, model = FDPLoader.load_fdp_to_db(package, get_engine(), send_progress)
        response = {
            'model_name': model_name,
            'babbage_model': model,
            'package': package_contents
        }
        send_progress(status='done', data=response)
        print("Finished to load %s" % package)
    except Exception as e:
        do_request(callback, package, 'fail', error=repr(e))
        print("Failed to load %s: %r" % (package, e))

