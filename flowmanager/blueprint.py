import logging
import requests
from flask import Blueprint, request
from flask_jsonpify import jsonpify
from auth.lib import Verifyer

from .models import FlowRegistry

from .controllers import upload, update, info
from .config import auth_server, db_connection_string


def make_blueprint():
    """Create blueprint.
    """

    verifyer = Verifyer(auth_endpoint=f'http://{auth_server}/auth/public-key')
    registry = FlowRegistry(db_connection_string)

    # Create instance
    blueprint = Blueprint('flowmanager', 'flowmanager')

    # Controller Proxies
    upload_controller = upload
    info_controller = info
    update_controller = update

    def upload_():
        token = request.headers.get('auth-token') or request.values.get('jwt')
        contents = request.get_json()
        return jsonpify(upload_controller(token, contents, registry, verifyer))

    def update_():
        contents = request.get_json()
        return jsonpify(update_controller(contents, registry))

    def info_(owner, dataset, revision):
        return jsonpify(info_controller(owner, dataset, revision, registry))

    # Register routes
    blueprint.add_url_rule(
        'upload', 'upload', upload_, methods=['POST'])
    blueprint.add_url_rule(
        'update', 'upadte', update_, methods=['POST'])
    blueprint.add_url_rule(
        '<owner>/<dataset>/<revision>', 'info', info_, methods=['GET'])

    # Return blueprint
    return blueprint
