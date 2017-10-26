import requests
from flask import Blueprint, request
from flask_jsonpify import jsonpify

from .models import FlowRegistry

from .controllers import upload, update, status, info
from .config import auth_server, db_connection_string


def make_blueprint():
    """Create blueprint.
    """

    public_key = requests.get(f'http://{auth_server}/auth/public-key').content
    registry = FlowRegistry(db_connection_string)

    # Create instance
    blueprint = Blueprint('flowmanager', 'flowmanager')

    # Controller Proxies
    upload_controller = upload
    status_controller = status
    info_controller = info
    update_controller = update

    def upload_():
        token = request.headers.get('auth-token') or request.values.get('jwt')
        contents = request.get_json()
        return jsonpify(upload_controller(token, contents, registry, public_key))

    def update_():
        contents = request.get_json()
        return jsonpify(update_controller(contents, registry))

    def status_(owner, dataset):
        return jsonpify(status_controller(owner, dataset, registry))

    def info_(owner, dataset):
        return jsonpify(info_controller(owner, dataset, registry))

    # Register routes
    blueprint.add_url_rule(
        'upload', 'upload', upload_, methods=['POST'])
    blueprint.add_url_rule(
        'update', 'upadte', update_, methods=['POST'])
    blueprint.add_url_rule(
        '<owner>/<dataset>/info', 'info', info_, methods=['GET'])
    blueprint.add_url_rule(
        '<owner>/<dataset>/status', 'status', status_, methods=['GET'])

    # Return blueprint
    return blueprint
