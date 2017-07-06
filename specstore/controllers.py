import jwt
from datapackage_pipelines_sourcespec_registry.registry import SourceSpecRegistry
from werkzeug.exceptions import Unauthorized, BadRequest, NotFound

from .config import owner_extractor, id_extractor, dpp_module


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
    uuid = None
    if contents is not None:
        owner = owner_extractor(contents)
        if owner is not None:
            if _verify(token, owner, public_key):
                try:
                    requested_uuid=id_extractor(contents)
                    if requested_uuid is not None:
                        spec = registry.get_source_spec(requested_uuid)
                        if spec is not None:
                            assert spec.owner == owner
                    uuid = registry.put_source_spec(owner, dpp_module, contents, uuid=requested_uuid)
                except ValueError as e:
                    errors.append('Validation failed for contents')
                except AssertionError:
                    errors.append('Unauthorized to update spec')
            else:
                errors.append('No token or token not authorised for owner')
        else:
            errors.append('Missing owner in spec')
    else:
        errors.append('Received empty contents (make sure your content-type is correct)')

    return {
        'success': len(errors) == 0,
        'id': uuid,
        'errors': errors
    }


def status(identifier, registry: SourceSpecRegistry):
    spec = registry.get_source_spec(identifier)
    if spec is None:
        raise NotFound()
    return {
        "state": "loaded"
    }
