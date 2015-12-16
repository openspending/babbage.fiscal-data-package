from flask import Blueprint, request, abort
from jinja2 import TemplateNotFound

from .loader import FDPLoader

babbage_loader = Blueprint('babbage_loader', __name__)
fdp_loader = FDPLoader()


@babbage_loader.route('/')
def load():
    try:
        package = request.args.get('package')
        callback = request.args.get('callback')
        fdp_loader.start_loading_in_bg(package, callback)
        return ""
    except TemplateNotFound:
        abort(400)
