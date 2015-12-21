from flask import Blueprint, request, abort
from .tasks import load_fdp_task
from .config import get_connection_string

FDPLoaderBlueprint = Blueprint('FDPLoader', __name__)


@FDPLoaderBlueprint.route('/')
def load():
    package = request.args.get('package')
    callback = request.args.get('callback')
    if package is not None and callback is not None:
        load_fdp_task.delay(package, callback, get_connection_string())
        return ""
    else:
        abort(400)
