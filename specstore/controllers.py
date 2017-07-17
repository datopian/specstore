import jwt
from datapackage_pipelines_sourcespec_registry.registry import SourceSpecRegistry
from werkzeug.exceptions import NotFound

from .config import dpp_module
from .config import dataset_getter, id_setter, owner_getter


def _verify(auth_token, owner, public_key):
    """Verify Auth Token.
    :param auth_token: Authentication token to verify
    :param owner: dataset owner
    """
    if not auth_token or not owner:
        return False
    try:
        token = jwt.decode(auth_token.encode('ascii'),
                           public_key,
                           algorithm='RS256')
        # TODO: check service in the future
        has_permission = True
        # has_permission = token.get('permissions', {}) \
        #     .get('datapackage-upload', False)
        # service = token.get('service')
        # has_permission = has_permission and service == 'os.datastore'
        has_permission = has_permission and owner == token.get('userid')
        return has_permission
    except jwt.InvalidTokenError:
        return False


def upload(token, contents, registry: SourceSpecRegistry, public_key):
    errors = []
    uid = None
    if contents is not None:
        owner = owner_getter(contents)
        if owner is not None:
            if _verify(token, owner, public_key):
                try:
                    dataset_name = dataset_getter(contents)
                    uid = registry.put_source_spec(dataset_name, owner, dpp_module, contents,
                                                   ignore_missing=True)
                except ValueError as e:
                    errors.append('Validation failed for contents')
            else:
                errors.append('No token or token not authorised for owner')
        else:
            errors.append('Missing owner in spec')
    else:
        errors.append('Received empty contents (make sure your content-type is correct)')

    return {
        'success': len(errors) == 0,
        'id': uid,
        'errors': errors
    }


def status(owner, dataset, registry: SourceSpecRegistry):
    spec = registry.get_source_spec(SourceSpecRegistry.format_uid(owner, dataset))
    if spec is None:
        raise NotFound()
    return {
        "state": "loaded"
    }
