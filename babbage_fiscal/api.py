from flask import Blueprint, request, abort
from jinja2 import TemplateNotFound

from .loader import FDPLoader

FDPLoaderBlueprint = Blueprint('FDPLoader', __name__)

_fdp_loader = None


def _get_fdp_loader():
    global _fdp_loader
    if _fdp_loader is None:
        _fdp_loader = FDPLoader()
    return _fdp_loader


@FDPLoaderBlueprint.route('/')
def load():
    package = request.args.get('package')
    callback = request.args.get('callback')
    if package is not None and callback is not None:
        _get_fdp_loader().start_loading_in_bg(package, callback)
        return ""
    else:
        abort(400)
