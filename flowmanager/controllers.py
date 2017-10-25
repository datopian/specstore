import datetime
import jwt
import requests

import planner
from werkzeug.exceptions import NotFound

from .config import dpp_module, dpp_server
from .config import dataset_getter, owner_getter, update_time_setter
from .models import FlowRegistry


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
                        dataset_id, now, 'flow-pending', errors)
                    revision = revision['revision']
                    pipelines = planner.plan(revision, contents)
                    for pipeline in pipelines:
                        doc = dict(
                            pipeline_id=registry.format_identifier(
                                owner, dataset_name, revision, pipeline[0]),
                            flow_id=registry.format_identifier(
                                owner, dataset_name, revision),
                            pipeline_details=pipeline,
                            status='pending',
                            errors=errors,
                            updated_at=now
                        )
                        registry.save_pipeline(doc)
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
    errors = content['errors']
    now = datetime.datetime.now()
    pipeline_id = content['pipeline']
    event = content['event']
    pipeline_status = 'pending'
    if content['success'] and event == 'finished':
        pipeline_status = 'success'
    elif (not content['success']) and  event == 'finished':
        pipeline_status = 'failed'
    doc = dict(
        status=pipeline_status,
        errors=errors,
        updated_at=now
    )
    registry.update_pipeline(pipeline_id, doc)
    flow_id = registry.get_flow_id(pipeline_id)
    flow_status = registry.check_flow_status(flow_id)
    doc['status'] = flow_status
    registry.update_revision(flow_id, doc)
    if flow_status == 'success':
        registry.delete_pipelines(flow_id)

    return {
        'status': flow_status,
        'id': flow_id,
        'errors': errors
    }


def get_fixed_pipeline_state(owner, dataset, registry: FlowRegistry):
    spec = registry.get_dataset(FlowRegistry.format_identifier(owner, dataset))
    if spec is None:
        raise NotFound()
    resp = requests.get(dpp_server + 'api/raw/{}/{}'.format(owner, dataset))
    if resp.status_code != 200:
        resp = {
            'state': 'LOADED'
        }
    else:
        resp = resp.json()
        update_time = resp.get('pipeline', {}).get('update_time')
        if update_time is None:
            update_time = ''
        if spec['updated_at'] and spec['updated_at'].isoformat() > update_time:
            resp['state'] = 'REGISTERED'
    resp['spec_contents'] = spec['spec']
    resp['spec_modified'] = spec['updated_at'].isoformat()
    return resp


def status(owner, dataset, registry: FlowRegistry):
    resp = get_fixed_pipeline_state(owner, dataset, registry)
    logs = resp.get('reason', '')
    if logs is None:
        logs = ''
    logs = logs.split('\n')[-50:],
    return {
        'state': resp['state'],
        'modified': resp.get('pipeline', {}).get('update_time'),
        'logs': logs,
        'stats': resp.get('stats', {})
    }


def info(owner, dataset, registry: FlowRegistry):
    resp = get_fixed_pipeline_state(owner, dataset, registry)
    return resp
