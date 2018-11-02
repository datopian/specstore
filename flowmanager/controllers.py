import datetime

import auth
import jwt
import requests
import logging
import yaml

import planner
import events
from datahub_emails import api as statuspage
from werkzeug.exceptions import NotFound
from dpp_runner.lib import DppRunner

from .schedules import parse_schedule
from .config import dpp_module
from .config import dataset_getter, owner_getter, update_time_setter, create_time_setter
from .config import verbosity
from .datasets import send_dataset
from .models import FlowRegistry, STATE_PENDING, STATE_SUCCESS, STATE_FAILED, STATE_RUNNING
from .models import get_descriptor

CONFIGS = {'allowed_types': [
    'derived/report',
    'derived/csv',
    'derived/json',
    'derived/zip',
    'derived/preview',
    'source/tabular',
    'source/non-tabular',
    'original'
]}

runner = DppRunner(max_workers=3)


def _internal_upload(owner, contents, registry, config=CONFIGS):
    errors = []
    dataset_name = dataset_getter(contents)
    now = datetime.datetime.now()
    update_time_setter(contents, now)

    flow_id = None
    dataset_id = registry.format_identifier(owner, dataset_name)
    dataset_obj = registry.create_or_update_dataset(dataset_id, owner, contents, now)
    create_time_setter(contents, dataset_obj.get('created_at'))
    period_in_seconds, schedule_errors = parse_schedule(contents)
    if len(schedule_errors) == 0:
        registry.update_dataset_schedule(dataset_id, period_in_seconds, now)

        revision = registry.create_revision(
            dataset_id, now, STATE_PENDING, errors)

        revision = revision['revision']
        flow_id=registry.format_identifier(
            owner, dataset_name, revision)
        pipelines = planner.plan(revision, contents, **config)
        pipeline_spec = dict(pipelines)
        for pipeline_id, pipeline_details in pipeline_spec.items():
            pipeline_spec[pipeline_id]
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

        runner.start(None, yaml.dump(pipeline_spec).encode('utf-8'),
                     status_cb=PipelineStatusCallback(registry), verbosity=verbosity)
    else:
        errors.extend(schedule_errors)
    return dataset_id, flow_id, errors


def upload(token, contents,
           registry: FlowRegistry,
           verifyer: auth.lib.Verifyer,
           config=CONFIGS):
    errors = []
    dataset_id = None
    flow_id = None
    if contents is not None:
        owner = owner_getter(contents)
        if owner is not None:
            permissions = verifyer.extract_permissions(token)
            if permissions and permissions.get('userid') == owner:
                limits = permissions.get('permissions')
                max_datasets = limits.get('max_dataset_num', 0)
                current_datasets = registry.num_datasets_for_owner(owner)
                dataset_id = registry.format_identifier(owner, dataset_getter(contents))
                is_revision = registry.get_dataset(dataset_id) is not None
                if current_datasets < max_datasets or is_revision:
                    try:
                        dataset_id, flow_id, errors = _internal_upload(owner, contents, registry, config=config)
                    except ValueError:
                        errors.append('Validation failed for contents')
                    except Exception as error:
                        errors.append('Unexpected error: %s' % error)
                else:
                    errors.append('Max datasets for user exceeded plan limit (%d)' % max_datasets)
            else:
                errors.append('No token or token not authorised for owner')
        else:
            errors.append('Missing owner in spec')
    else:
        errors.append('Received empty contents (make sure your content-type is correct)')

    if len(errors) and contents is not None:
        statuspage.on_incident('Failed To Start Pipelines', contents.get('meta', {}).get('owner'), errors)

    return {
        'success': len(errors) == 0,
        'dataset_id': dataset_id,
        'flow_id': flow_id,
        'errors': errors
    }


class PipelineStatusCallback:
    def __init__(self, flowregistry: FlowRegistry):
        self.registry = flowregistry

    def __call__(self, pipeline_id, state, errors=None, stats=None): #noqa
        logging.info('Status %s: %s (errors#=%d, stats=%r)',
                     pipeline_id, state, len(errors) if errors is not None else 0,
                     stats)
        now = datetime.datetime.now()
        registry = self.registry

        if pipeline_id.startswith('./'):
            pipeline_id = pipeline_id[2:]

        errors = errors
        if state in ('SUCCESS', 'FAILED'):
            event = 'finish'
        else:
            event = 'progress'
        success = state == 'SUCCESS'
        log = []
        stats = stats if stats is not None else {}


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
            pipeline = registry.get_pipeline(pipeline_id)
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
            dataset = registry.get_dataset(revision['dataset_id'])
            if (flow_status != STATE_PENDING) and (flow_status != STATE_RUNNING):
                registry.delete_pipelines(flow_id)
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
            if flow_status == STATE_FAILED:
                statuspage.on_incident('Pipelines Failed', dataset['spec']['meta']['owner'], errors)

            no_succesful_revision = registry.get_revision(revision['dataset_id'], 'successful') is None

            if flow_status == STATE_SUCCESS or no_succesful_revision:
                descriptor : dict = get_descriptor(flow_id)
                if descriptor is not None:
                    if no_succesful_revision and descriptor['datahub'].get('findability') == 'published':
                        descriptor['datahub']['findability'] = 'unlisted'
                    send_dataset(
                        descriptor.get('id'),
                        descriptor.get('name'),
                        descriptor.get('title'),
                        descriptor.get('description'),
                        descriptor.get('datahub'),
                        descriptor,
                        dataset.get('certified') or False)

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
    pipelines = revision['pipelines']
    resp = dict(
        id = revision['revision_id'],
        spec_contents=spec['spec'],
        modified=spec['updated_at'].isoformat(),
        state=state,
        error_log=revision['errors'],
        logs=revision['logs'],
        stats=revision['stats'],
        pipelines=pipelines if pipelines is not None else {},
        certified=spec.get('certified')
    )
    return resp


## helpers


def update_dependants(flow_id, pipeline_id, registry):
    cb = PipelineStatusCallback(registry)
    for queued_pipeline in \
        registry.list_pipelines_by_flow_and_status(flow_id):
        for dep in queued_pipeline.pipeline_details.get('dependencies', []):
            if dep['pipeline'].lstrip('./') == pipeline_id:
                cb(queued_pipeline.pipeline_id,
                   'FAILED',
                   errors=[
                    'Dependency unsuccessful. '
                    'Cannot run until dependency "{}" is successfully'
                    'executed'.format(pipeline_id)
                   ])
