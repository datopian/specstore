import datetime
import jwt
import requests

import planner
from werkzeug.exceptions import NotFound

from .config import dpp_module, dpp_server
from .config import dataset_getter, owner_getter, update_time_setter
from .models import FlowRegistry, STATE_PENDING, STATE_SUCCESS, STATE_FAILED

CONFIGS = {'allowed_types': [
    'derived/csv',
    'derived/json',
    'derived/zip',
    'source/tabular',
    'source/non-tabular'
]}


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


def upload(token, contents, registry: FlowRegistry, public_key):
    errors = []
    dataset_id = None
    if contents is not None:
        owner = owner_getter(contents)
        if owner is not None:
            if _verify(token, owner, public_key):
                try:
                    dataset_name = dataset_getter(contents)
                    now = datetime.datetime.now()
                    update_time_setter(contents, now)

                    dataset_id = registry.format_identifier(owner, dataset_name)
                    registry.create_or_update_dataset(
                        dataset_id, owner, contents, now)

                    revision = registry.create_revision(
                        dataset_id, now, STATE_PENDING, errors)

                    revision = revision['revision']
                    pipelines = planner.plan(revision, contents, **CONFIGS)
                    for pipeline_id, pipeline_details in pipelines:
                        doc = dict(
                            pipeline_id=pipeline_id,
                            flow_id=registry.format_identifier(
                                owner, dataset_name, revision),
                            pipeline_details=pipeline_details,
                            status=STATE_PENDING,
                            errors=errors,
                            logs=[],
                            stats={},
                            created_at=now,
                            updated_at=now
                        )
                        registry.save_pipeline(doc)

                    if dpp_server:
                        if requests.get(dpp_server + 'api/refresh').status_code != 200:
                            errors.append('Failed to refresh pipelines status')

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
        'id': dataset_id,
        'errors': errors
    }


def update(content, registry: FlowRegistry):

    now = datetime.datetime.now()

    pipeline_id = content['pipeline_id']
    if pipeline_id.startswith('./'):
        pipeline_id = pipeline_id[2:]

    errors = content.get('errors')
    event = content['event']
    success = content.get('success')
    log = content.get('log', [])
    stats = content.get('stats', {})

    pipeline_status = STATE_PENDING
    if event == 'finish':
        if success:
            pipeline_status = STATE_SUCCESS
        else:
            pipeline_status = STATE_FAILED

    doc = dict(
        status=pipeline_status,
        errors=errors,
        stats=stats,
        log=log,
        updated_at=now
    )
    if registry.update_pipeline(pipeline_id, doc):
        flow_id = registry.get_flow_id(pipeline_id)
        flow_status = registry.check_flow_status(flow_id)
        doc = dict(
            status = flow_status,
            updated_at=now
        )
        if errors:
            doc['errors'] = errors
        if stats:
            doc['stats'] = stats
        if log:
            doc['logs'] = log
        registry.update_revision(flow_id, doc)
        if flow_status != STATE_PENDING:
            registry.delete_pipelines(flow_id)

        return {
            'status': flow_status,
            'id': flow_id,
            'errors': errors
        }
    else:
        return {
            'status': None,
            'id': None,
            'errors': ['pipeline not found']
        }


def get_fixed_pipeline_state(owner, dataset, registry: FlowRegistry):
    dataset_id = FlowRegistry.format_identifier(owner, dataset)
    spec = registry.get_dataset(dataset_id)
    if spec is None:
        raise NotFound()
    revision = registry.get_revision_by_dataset_id(dataset_id)
    if revision is None:
        raise NotFound()
    state = {
        STATE_PENDING: 'QUEUED',
        STATE_SUCCESS: 'SUCCEEDED',
        STATE_FAILED: 'FAILED',
    }[revision['status']]
    resp = dict(
        spec_contents=spec['spec'],
        modified=spec['updated_at'].isoformat(),
        state=state,
        error_log=revision['errors'],
        logs=revision['logs'],
        stats=revision['stats']
    )
    return resp


def status(owner, dataset, registry: FlowRegistry):
    resp = get_fixed_pipeline_state(owner, dataset, registry)
    del resp['logs']
    del resp['spec_contents']
    return resp


def info(owner, dataset, registry: FlowRegistry):
    resp = get_fixed_pipeline_state(owner, dataset, registry)
    return resp
