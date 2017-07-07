import requests
from flask import Blueprint, request
from flask_jsonpify import jsonpify

from datapackage_pipelines_sourcespec_registry.registry import SourceSpecRegistry

from .controllers import upload, status
from .config import auth_server, db_connection_string


def make_blueprint():
    """Create blueprint.
    """

    public_key = requests.get(f'http://{auth_server}/auth/public-key').content
    registry = SourceSpecRegistry(db_connection_string)

    # Create instance
    blueprint = Blueprint('specstore', 'specstore')

    # Controller Proxies
    upload_controller = upload
    status_controller = status

    def upload_():
        token = request.headers.get('auth-token')
        contents = request.get_json()
        return jsonpify(upload_controller(token, contents, registry, public_key))

    def status_(identifier):
        return jsonpify(status_controller(identifier, registry))

    # Register routes
    blueprint.add_url_rule(
        'upload', 'upload', upload_, methods=['POST'])
    blueprint.add_url_rule(
        '<identifier>/status', 'status', status_, methods=['GET'])

    # Return blueprint
    return blueprint
