from celery import Celery
import requests

from .config import get_engine, _set_connection_string
from .loader import FDPLoader

app = Celery('fdp_loader')
app.config_from_object('babbage_fiscal.celeryconfig')


def do_request(callback, package, status, progress=None, error=None):
    params = {'package': package, 'status': status}
    if progress is not None:
        params['progress'] = progress
    if error is not None:
        params['error'] = error
    requests.get(callback, params).content

@app.task
def load_fdp_task(package, callback, connection_string=None):
    def send_progress(count):
        do_request(callback, package, 'progress', count)
    if connection_string is not None:
        _set_connection_string(connection_string)
    try:
        print("Starting to load %s" % package)
        do_request(callback, package, 'progress', 0)
        FDPLoader.load_fdp_to_db(package, get_engine(), send_progress)
        do_request(callback, package, 'done')
        print("Finished to load %s" % package)
    except Exception as e:
        do_request(callback, package, 'error', error=repr(e))
        print("Failed to load %s: %r" % (package, e))

