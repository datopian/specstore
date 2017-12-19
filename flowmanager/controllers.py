import datetime
import jwt
import requests
import logging

import planner
import events
from werkzeug.exceptions import NotFound

from .schedules import parse_schedule
from .config import dpp_module, dpp_server
from .config import dataset_getter, owner_getter, update_time_setter
from .models import FlowRegistry, STATE_PENDING, STATE_SUCCESS, STATE_FAILED, STATE_RUNNING

CONFIGS = {'allowed_types': [
    'derived/report',
    'derived/csv',
    'derived/json',
    'derived/zip',
    'derived/preview',
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


def _internal_upload(owner, contents, registry, config=CONFIGS):
    errors = []
    dataset_name = dataset_getter(contents)
    now = datetime.datetime.now()
    update_time_setter(contents, now)

    flow_id = None
    dataset_id = registry.format_identifier(owner, dataset_name)
    registry.create_or_update_dataset(
        dataset_id, owner, contents, now)
    period_in_seconds, schedule_errors = parse_schedule(contents)
    if len(schedule_errors) == 0:
        registry.update_dataset_schedule(dataset_id, period_in_seconds, now)

        revision = registry.create_revision(
            dataset_id, now, STATE_PENDING, errors)

        revision = revision['revision']
        flow_id=registry.format_identifier(
            owner, dataset_name, revision)
        pipelines = planner.plan(revision, contents, **config)
        for pipeline_id, pipeline_details in pipelines:
            doc = dict(
                pipeline_id=pipeline_id,
                flow_id=flow_id,
                title=pipeline_details.get('title'),
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
    else:
        errors.extend(schedule_errors)
    return dataset_id, flow_id, errors


def upload(token, contents, registry: FlowRegistry, public_key, config=CONFIGS):
    errors = []
    dataset_id = None
    flow_id = None
    if contents is not None:
        owner = owner_getter(contents)
        if owner is not None:
            if _verify(token, owner, public_key):
                try:
                    dataset_id, flow_id, errors = _internal_upload(owner, contents, registry, config=config)
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
        'dataset_id': dataset_id,
        'flow_id': flow_id,
        'errors': errors
    }


def update(content, registry: FlowRegistry): #noqa
    now = datetime.datetime.now()

    pipeline_id = content['pipeline_id']
    if pipeline_id.startswith('./'):
        pipeline_id = pipeline_id[2:]

    errors = content.get('errors')
    event = content['event']
    success = content.get('success')
    log = content.get('log', [])
    stats = content.get('stats', {})


    pipeline_status = STATE_PENDING if event=='queue' else STATE_RUNNING
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

        if pipeline_status == STATE_FAILED:
            update_dependants(flow_id, pipeline_id, registry)

        doc = dict(
            status = flow_status,
            updated_at=now,
        )
        if errors:
            doc['errors'] = errors
        if stats:
            doc['stats'] = stats
        if log:
            doc['logs'] = log

        rev = registry.get_revision_by_revision_id(flow_id)
        pipeline = registry.get_pipeline(pipeline_id)
        pipelines = rev.get('pipelines')
        if pipelines is None:
            pipelines = {}

        pipeline_state = {
            STATE_PENDING: 'QUEUED',
            STATE_RUNNING: 'INPROGRESS',
            STATE_SUCCESS: 'SUCCEEDED',
            STATE_FAILED: 'FAILED',
        }[pipeline_status]

        pipelines[pipeline_id] = dict(
            title=pipeline.get('title'),
            status=pipeline_state,
            stats=stats,
            error_log=errors,
        )
        doc['pipelines'] = pipelines
        revision = registry.update_revision(flow_id, doc)
        if (flow_status != STATE_PENDING) and (flow_status != STATE_RUNNING):
            registry.delete_pipelines(flow_id)

            dataset = registry.get_dataset(revision['dataset_id'])
            findability = \
                flow_status == STATE_SUCCESS and \
                dataset['spec']['meta']['findability'] == 'published'
            findability = 'published' if findability else 'private'
            events.send_event(
                'flow',       # Source of the event
                event,       # What happened
                'OK' if flow_status == STATE_SUCCESS else 'FAIL',       # Success indication
                findability,  # one of "published/private/internal":
                dataset['owner'],       # Actor
                dataset_getter(dataset['spec']),   # Dataset in question
                dataset['spec']['meta']['owner'],      # Owner of the dataset
                dataset['spec']['meta']['ownerid'],      # Ownerid of the dataset
                flow_id,      # Related flow id
                pipeline_id,  # Related pipeline id
                {
                    'flow-id': flow_id,
                    'errors': errors,

                }       # Other payload
            )

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


def info(owner, dataset, revision_id, registry: FlowRegistry):
    dataset_id = FlowRegistry.format_identifier(owner, dataset)
    spec = registry.get_dataset(dataset_id)
    if spec is None:
        raise NotFound()
    revision = registry.get_revision(dataset_id, revision_id)
    if revision is None:
        raise NotFound()
    state = {
        STATE_PENDING: 'QUEUED',
        STATE_RUNNING: 'INPROGRESS',
        STATE_SUCCESS: 'SUCCEEDED',
        STATE_FAILED: 'FAILED',
    }[revision['status']]
    resp = dict(
        id = revision['revision_id'],
        spec_contents=spec['spec'],
        modified=spec['updated_at'].isoformat(),
        state=state,
        error_log=revision['errors'],
        logs=revision['logs'],
        stats=revision['stats'],
        pipelines=revision['pipelines']
    )
    return resp


## helpers


def update_dependants(flow_id, pipeline_id, registry):
    for queued_pipeline in \
        registry.list_pipelines_by_flow_and_status(flow_id):
        for dep in queued_pipeline.pipeline_details.get('dependencies', []):
            if dep['pipeline'] == pipeline_id:
                content = dict(
                    pipeline_id=queued_pipeline.pipeline_id,
                    event='finish',
                    success=False,
                    errors=[
                        'Dependency unsuccessful. '
                        'Cannot run until dependency "{}" is successfully'
                        'executed'.format(pipeline_id)
                    ]
                )
                update(content, registry)
