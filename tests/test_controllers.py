import datetime
import json

import auth
import jwt
import pytest
import os
import requests
import time

from flowmanager.models import FlowRegistry, get_descriptor, get_s3_client
from werkzeug.exceptions import NotFound
import requests_mock

from .config import load_spec

import flowmanager.controllers
upload = flowmanager.controllers.upload
update = flowmanager.controllers.update
info = flowmanager.controllers.info
flowmanager.controllers.dpp_server = 'http://dpp/'

os.environ['PKGSTORE_BUCKET'] = 'testing.bucket.com'

private_key = open('tests/private.pem').read()
public_key = open('tests/public.pem').read()
spec = load_spec('simple')
spec2 =  load_spec('simple2')
spec_unauth = load_spec('unauth')

now = datetime.datetime.now()

def generate_token(owner):
    ret = {
        'userid': owner,
        'permissions': {
            'max_dataset_num': 10
        },
        'service': 'source'
    }
    token = jwt.encode(ret, private_key, algorithm='RS256').decode('ascii')
    return token


@pytest.fixture
def empty_registry():
    r = FlowRegistry('sqlite://')
    return r


@pytest.fixture
def full_registry():
    r = FlowRegistry('sqlite://')
    r.save_dataset(dict(identifier='me/id', owner='me', spec=spec, updated_at=now))
    r.save_dataset(dict(identifier='you/id', owner='you', spec=spec, updated_at=now))
    r.save_dataset_revision(dict(
        revision_id='me/id/1',
        dataset_id='me/id',
        revision=1,
        status='pending',
        logs=[]))
    r.save_dataset_revision(dict(
        revision_id='you/id/1',
        dataset_id='you/id',
        revision=1,
        status='pending',
        logs=[]))
    r.save_dataset_revision(dict(
        revision_id='you/id/2',
        dataset_id='you/id',
        revision=2,
        status='success',
        logs=['this is latest successful']))
    r.save_dataset_revision(dict(
        revision_id='you/id/3',
        dataset_id='you/id',
        revision=3,
        status='pending',
        logs=['this is latest']))
    r.save_pipeline(dict(
        pipeline_id='me/id:non-tabular',
        flow_id='me/id/1',
        pipeline_details={},
        status='pending',
        logs=[],
        title='Copying source data'))
    r.save_pipeline(dict(
        pipeline_id='me/id',
        flow_id='me/id/1',
        pipeline_details={},
        status='pending',
        logs=[],
        title='Creating Package'))
    return r

@pytest.fixture
def full_registry_with_deps():
    r = FlowRegistry('sqlite://')
    r.save_dataset(dict(identifier='me/id', owner='me', spec=spec, updated_at=now))
    r.save_dataset_revision(dict(
        revision_id='me/id/1',
        dataset_id='me/id',
        revision=1,
        status='pending',
        logs=[]))
    r.save_pipeline(dict(
        pipeline_id='me/id:csv',
        flow_id='me/id/1',
        pipeline_details={'dependencies': []},
        status='pending',
        logs=[],
        title='Creating CSV'))
    r.save_pipeline(dict(
        pipeline_id='me/id:json',
        flow_id='me/id/1',
        pipeline_details={'dependencies': []},
        status='pending',
        logs=[],
        title='Creating JSON'))
    r.save_pipeline(dict(
        pipeline_id='me/id:zip',
        flow_id='me/id/1',
        pipeline_details={'dependencies': [{'pipeline': 'me/id:csv'}]},
        status='pending',
        logs=[],
        title='Creating ZIP'))
    r.save_pipeline(dict(
        pipeline_id='me/id:preview',
        flow_id='me/id/1',
        pipeline_details={'dependencies': [{'pipeline': 'me/id:json'}]},
        status='pending',
        logs=[],
        title='Generating Preview'))
    r.save_pipeline(dict(
        pipeline_id='me/id',
        flow_id='me/id/1',
        pipeline_details={'dependencies': [
            {'pipeline': 'me/id:csv'},
            {'pipeline': 'me/id:json'},
            {'pipeline': 'me/id:preview'},
            {'pipeline': 'me/id:zip'}]},
        status='pending',
        logs=[],
        title='Creating Package'))
    return r

# STATUS

def test_info_not_found(empty_registry):
    with pytest.raises(NotFound):
        info('me', 'id', 1, empty_registry)


def test_info_found_no_pipeline(full_registry):
    ret = info('me', 'id', 1, full_registry)
    assert ret['state'] == "QUEUED"


def test_info_found_has_pipeline(full_registry):
    response = dict(
        id='me/id/1',
        state='QUEUED',
        spec_contents=spec,
        modified=now.isoformat(),
        error_log=None,
        logs=[],
        stats=None,
        pipelines=None
    )
    ret = info('me', 'id', 1, full_registry)
    assert ret == response


def test_info_found_has_pipeline_current(full_registry):
    ret = info('me', 'id', 1, full_registry)
    assert ret == {
        'id': 'me/id/1',
        'state': 'QUEUED',
        'modified': now.isoformat(),
        'logs': [],
        'error_log': None,
        'spec_contents': spec,
        'stats': None,
        'pipelines': None
    }

def test_grabs_info_for_given_revision_id(full_registry):
    ret = info('you', 'id', 2, full_registry)
    assert ret == {
        'id': 'you/id/2',
        'state': 'SUCCEEDED',
        'modified': now.isoformat(),
        'logs': ['this is latest successful'],
        'error_log': None,
        'spec_contents': spec,
        'stats': None,
        'pipelines': None
    }

def test_grabs_info_for_latest(full_registry):
    ret = info('you', 'id', 'latest', full_registry)
    assert ret == {
        'id': 'you/id/3',
        'state': 'QUEUED',
        'modified': now.isoformat(),
        'logs': ['this is latest'],
        'error_log': None,
        'spec_contents': spec,
        'stats': None,
        'pipelines': None
    }

def test_grabs_info_for_given_revision_id(full_registry):
    ret = info('you', 'id', 'successful', full_registry)
    assert ret == {
        'id': 'you/id/2',
        'state': 'SUCCEEDED',
        'modified': now.isoformat(),
        'logs': ['this is latest successful'],
        'error_log': None,
        'spec_contents': spec,
        'stats': None,
        'pipelines': None
    }


def test_updates_and_displays_info_with_pipelines(full_registry):
    ret = info('me', 'id', 'latest', full_registry)

    # Check empty
    assert ret['pipelines'] == None

    payload = {
      "pipeline_id": "me/id",
      "event": "progress",
      "success": True,
      "errors": [],
      "log": []
    }
    update(payload, full_registry)
    payload = {
      "pipeline_id": "me/id:non-tabular",
      "event": "progress",
      "success": True,
      "errors": [],
      "log": []
    }
    update(payload, full_registry)

    # check updated
    ret = info('me', 'id', 'latest', full_registry)
    assert ret['pipelines'] == {'me/id': {
            'status': 'INPROGRESS',
            'stats': {},
            'error_log': [],
            'title': 'Creating Package'
        },
        'me/id:non-tabular': {
            'status': 'INPROGRESS',
            'stats': {},
            'error_log': [],
            'title': 'Copying source data'
        }
    }

    payload = {
      "pipeline_id": "me/id",
      "event": "finish",
      "success": True,
      "errors": [],
      "stats": {'count': 1}
    }
    update(payload, full_registry)
    payload = {
      "pipeline_id": "me/id:non-tabular",
      "event": "finish",
      "success": False,
      "errors": ['an', 'error', 'log'],
      "log": []
    }
    update(payload, full_registry)

    # upadte and check again
    ret = info('me', 'id', 'latest', full_registry)
    assert ret['pipelines'] == {'me/id': {
            'status': 'SUCCEEDED',
            "stats": {'count': 1},
            'error_log': [],
            'title': 'Creating Package'
        },
        'me/id:non-tabular': {
            'status': 'FAILED',
            'stats': {},
            'error_log': ['an', 'error', 'log'],
            'title': 'Copying source data'
        }
    }

    # check flow status is failed
    assert ret['state'] == 'FAILED'

def test_all_pipeline_statuses_are_updated_if_failed(full_registry):
    payload = {
        "pipeline_id": "me/id",
        "event": "progress",
        "success": True,
        "errors": [],
        "log": ["a", "log", "line"]
    }
    update(payload, full_registry)
    payload = {
      "pipeline_id": "me/id",
      "event": "finish",
      "success": False,
      "errors": ['error']
    }
    update(payload, full_registry)
    payload = {
      "pipeline_id": "me/id:non-tabular",
      "event": "finish",
      "success": True,
      "errors": []
    }
    update(payload, full_registry)

    ret = info('me', 'id', 'latest', full_registry)

    assert ret['pipelines'] == {'me/id:non-tabular': {
            'status': 'SUCCEEDED',
            "stats": {},
            'error_log': [],
            'title': 'Copying source data'
        },
        'me/id': {
            'status': 'FAILED',
            'stats': {},
            'error_log': ['error'],
            'title': 'Creating Package'
        }
    }



# UPLOAD

def test_upload_no_contents(empty_registry):
    token = generate_token('me')
    ret = upload(token, None, empty_registry, auth.lib.Verifyer(public_key=public_key))
    assert not ret['success']
    assert ret['dataset_id'] is None
    assert ret['flow_id'] is None
    assert ret['errors'] == ['Received empty contents (make sure your content-type is correct)']


def test_upload_bad_contents(empty_registry):
    token = generate_token('me')
    ret = upload(token, {}, empty_registry, auth.lib.Verifyer(public_key=public_key))
    assert not ret['success']
    assert ret['dataset_id'] is None
    assert ret['flow_id'] is None
    assert ret['errors'] == ['Missing owner in spec']


def test_upload_no_token(empty_registry):
    ret = upload(None, spec, empty_registry, auth.lib.Verifyer(public_key=public_key))
    assert not ret['success']
    assert ret['dataset_id'] is None
    assert ret['flow_id'] is None
    assert ret['errors'] == ['No token or token not authorised for owner']


def test_upload_bad_token(empty_registry):
    token = generate_token('mee')
    ret = upload(token, spec, empty_registry, auth.lib.Verifyer(public_key=public_key))
    assert not ret['success']
    assert ret['dataset_id'] is None
    assert ret['flow_id'] is None
    assert ret['errors'] == ['No token or token not authorised for owner']


def test_upload_new(empty_registry: FlowRegistry):
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/refresh', status_code=200)
        token = generate_token('me')
        ret = upload(token, spec, empty_registry, auth.lib.Verifyer(public_key=public_key))
        assert ret['success'], repr(ret['errors'])
        assert ret['dataset_id'] == 'me/id'
        assert ret['flow_id'] == 'me/id/1'
        assert ret['errors'] == []
        specs = list(empty_registry.list_datasets())
        assert len(specs) == 1
        first = specs[0]
        assert first.owner == 'me'
        assert first.identifier == 'me/id'
        assert first.spec == spec
        revision = empty_registry.get_revision('me/id')
        assert revision['revision'] == 1
        assert revision['status'] == 'pending'
        pipelines = list(empty_registry.list_pipelines_by_id('me/id/1'))
        assert len(pipelines) == 7
        pipeline = pipelines[0]
        assert pipeline.status == 'pending'
        pipelines = list(empty_registry.list_pipelines())
        assert len(pipelines) == 7


def test_upload_existing(full_registry):
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/refresh', status_code=200)
        token = generate_token('me')
        ret = upload(token, spec, full_registry, auth.lib.Verifyer(public_key=public_key))
        assert ret['success'], repr(ret['errors'])
        assert ret['dataset_id'] == 'me/id'
        assert ret['flow_id'] == 'me/id/2'
        assert ret['errors'] == []
        specs = list(full_registry.list_datasets())
        assert len(specs) == 2
        first = specs[0]
        assert first.owner == 'me'
        assert first.identifier == 'me/id'
        assert first.spec == spec
        revision = full_registry.get_revision('me/id')
        assert revision['revision'] == 2
        assert revision['status'] == 'pending'
        pipelines = list(full_registry.list_pipelines_by_id('me/id/2'))
        assert len(pipelines) == 7
        pipeline = pipelines[0]
        assert pipeline.status == 'pending'
        ## make pipelines for previous revision are still there
        pipelines = list(full_registry.list_pipelines_by_id('me/id/1'))
        assert len(pipelines) == 2
        pipelines = list(full_registry.list_pipelines())
        assert len(pipelines) == 9


def test_upload_append(full_registry):
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/refresh', status_code=200)
        token = generate_token('me2')
        ret = upload(token, spec2, full_registry, auth.lib.Verifyer(public_key=public_key))
        assert ret['success'], repr(ret['errors'])
        assert ret['dataset_id'] == 'me2/id2'
        assert ret['flow_id'] == 'me2/id2/1'
        assert ret['errors'] == []
        specs = list(full_registry.list_datasets())
        assert len(specs) == 3
        first = specs[2]

        assert first.owner == 'me2'
        assert first.identifier == 'me2/id2'
        assert first.spec == spec2
        second = specs[0]
        assert second.owner == 'me'
        assert second.identifier == 'me/id'
        assert second.spec == spec

        revision = full_registry.get_revision('me2/id2')
        assert revision['revision'] == 1
        assert revision['status'] == 'pending'
        pipelines = list(full_registry.list_pipelines_by_id('me2/id2/1'))
        assert len(pipelines) == 7
        pipelines = list(full_registry.list_pipelines_by_id('me/id/1'))
        assert len(pipelines) == 2
        pipelines = list(full_registry.list_pipelines())
        assert len(pipelines) == 9

def test_update_running(full_registry):
    payload = {
      "pipeline_id": "me/id",
      "event": "finish",
      "success": True,
      "errors": [],
      "log": ["a", "log", "line"]
    }
    ret = update(payload, full_registry)
    assert ret['status'] == 'running'
    assert ret['id'] == 'me/id/1'
    revision = full_registry.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'running'
    assert revision['logs'] == ["a", "log", "line"]

    # pipeline details
    assert revision['pipelines']['me/id']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id']['stats'] == {}
    assert revision['pipelines']['me/id']['error_log'] == []
    assert revision['pipelines']['me/id']['title'] == 'Creating Package'

def test_update_fail(full_registry):
    payload = {
        "pipeline_id": "me/id",
        "event": "progress",
        "success": True,
        "errors": [],
        "log": ["a", "log", "line"]
    }
    update(payload, full_registry)
    payload = {
      "pipeline_id": "me/id",
      "event": "finish",
      "success": False,
      "errors": ['error']
    }
    ret = update(payload, full_registry)
    payload = {
      "pipeline_id": "me/id:non-tabular",
      "event": "finish",
      "success": True,
      "errors": ['error']
    }
    ret = update(payload, full_registry)
    assert ret['status'] == 'failed'
    assert ret['id'] == 'me/id/1'
    revision = full_registry.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'failed'
    assert revision['logs'] == ["a", "log", "line"]

    # pipeline details
    assert revision['pipelines']['me/id']['status'] == 'FAILED'
    assert revision['pipelines']['me/id']['stats'] == {}
    assert revision['pipelines']['me/id']['error_log'] == ['error']
    assert revision['pipelines']['me/id']['title'] == 'Creating Package'


def test_update_success(full_registry):
    datapackage= {
        "id": "datahub/dataset",
        "name": "testing-dataset",
        "title": "Testing Dataset",
        "description": "Test description",
        "datahub": {"owner": "owner", "stats": {"bytes": 1}}
    }
    s3 = get_s3_client()
    s3.put_object(
        Bucket=os.environ['PKGSTORE_BUCKET'],
        Key='me/id/1/datapackage.json',
        Body=json.dumps(datapackage))

    payload = {
      "pipeline_id": "me/id",
      "event": "finish",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry)
    assert ret['status'] == 'running'
    assert ret['id'] == 'me/id/1'
    revision = full_registry.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'running'

    payload = {
      "pipeline_id": "me/id:non-tabular",
      "event": "finish",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry)
    assert ret['status'] == 'success'
    assert ret['id'] == 'me/id/1'
    revision = full_registry.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'success'

    pipelines = full_registry.list_pipelines_by_id('me/id')
    assert len(list(pipelines)) == 0
    pipelines = full_registry.list_pipelines()
    assert len(list(pipelines)) == 0

    # pipeline details
    assert revision['pipelines']['me/id']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id']['stats'] == {}
    assert revision['pipelines']['me/id']['error_log'] == []
    assert revision['pipelines']['me/id']['title'] == 'Creating Package'

    assert revision['pipelines']['me/id:non-tabular']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id:non-tabular']['stats'] == {}
    assert revision['pipelines']['me/id:non-tabular']['error_log'] == []
    assert revision['pipelines']['me/id:non-tabular']['title'] == 'Copying source data'

    # Test exported to Elasticsearch
    time.sleep(5)
    res = requests.get('http://localhost:9200/datahub/_search')
    assert res.status_code == 200

    meta = res.json()
    hits = [hit['_source'] for hit in meta['hits']['hits']
        if hit['_source']['datapackage']['name'] == 'testing-dataset']

    assert len(hits) == 1

    exp = {
        "id": "datahub/dataset",
        "name": "testing-dataset",
        "title": "Testing Dataset",
        "description": "Test description",
        "datahub": {"owner": "owner", "stats": {"bytes": 1}},
        "datapackage": {
            "id": "datahub/dataset",
            "name": "testing-dataset",
            "title": "Testing Dataset",
            "description": "Test description",
            "datahub": {"owner": "owner", "stats": {"bytes": 1}}
        }
    }
    assert hits[0] == exp


def test_update_failed_with_deps(full_registry_with_deps):
    payload = {
      "pipeline_id": "me/id:json",
      "event": "finish",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry_with_deps)
    assert ret['status'] == 'running'
    revision = full_registry_with_deps.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'running'
    payload = {
      "pipeline_id": "me/id:csv",
      "event": "finish",
      "success": False,
      "errors": ['error']
    }
    ret = update(payload, full_registry_with_deps)
    assert ret['status'] == 'running'
    revision = full_registry_with_deps.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'running'

    payload = {
      "pipeline_id": "me/id:preview",
      "event": "finish",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry_with_deps)
    assert ret['status'] == 'failed'
    revision = full_registry_with_deps.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'failed'

    pipelines = full_registry_with_deps.list_pipelines_by_id('me/id/1')
    assert len(list(pipelines)) == 0
    pipelines = full_registry_with_deps.list_pipelines()
    assert len(list(pipelines)) == 0

    # pipeline details
    assert revision['pipelines']['me/id:csv']['status'] == 'FAILED'
    assert revision['pipelines']['me/id:csv']['stats'] == {}
    assert revision['pipelines']['me/id:csv']['error_log'] == ['error']

    assert revision['pipelines']['me/id:zip']['status'] == 'FAILED'
    assert revision['pipelines']['me/id:zip']['stats'] == {}
    assert revision['pipelines']['me/id:zip']['error_log'] == \
        ['Dependency unsuccessful. Cannot run until dependency "me/id:csv" is successfullyexecuted']

    assert revision['pipelines']['me/id']['status'] == 'FAILED'
    assert revision['pipelines']['me/id']['stats'] == {}
    assert revision['pipelines']['me/id']['error_log'] == \
        ['Dependency unsuccessful. Cannot run until dependency "me/id:csv" is successfullyexecuted']

    assert revision['pipelines']['me/id:json']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id:json']['stats'] == {}
    assert revision['pipelines']['me/id:json']['error_log'] == []

    assert revision['pipelines']['me/id:preview']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id:preview']['stats'] == {}
    assert revision['pipelines']['me/id:preview']['error_log'] == []


def test_update_success_with_deps(full_registry_with_deps):
    payload = {
      "pipeline_id": "me/id:json",
      "event": "finish",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry_with_deps)

    payload = {
      "pipeline_id": "me/id:csv",
      "event": "finish",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry_with_deps)

    payload = {
      "pipeline_id": "me/id:preview",
      "event": "finish",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry_with_deps)

    payload = {
      "pipeline_id": "me/id:zip",
      "event": "finish",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry_with_deps)
    assert ret['status'] == 'running'
    revision = full_registry_with_deps.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'running'

    payload = {
      "pipeline_id": "me/id",
      "event": "finish",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry_with_deps)
    assert ret['status'] == 'success'
    revision = full_registry_with_deps.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'success'

    pipelines = full_registry_with_deps.list_pipelines_by_id('me/id/1')
    assert len(list(pipelines)) == 0
    pipelines = full_registry_with_deps.list_pipelines()
    assert len(list(pipelines)) == 0

    # pipeline details
    assert revision['pipelines']['me/id:csv']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id:csv']['stats'] == {}
    assert revision['pipelines']['me/id:csv']['error_log'] == []

    assert revision['pipelines']['me/id:zip']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id:zip']['stats'] == {}
    assert revision['pipelines']['me/id:zip']['error_log'] == []

    assert revision['pipelines']['me/id']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id']['stats'] == {}
    assert revision['pipelines']['me/id']['error_log'] == []

    assert revision['pipelines']['me/id:json']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id:json']['stats'] == {}
    assert revision['pipelines']['me/id:json']['error_log'] == []

    assert revision['pipelines']['me/id:preview']['status'] == 'SUCCEEDED'
    assert revision['pipelines']['me/id:preview']['stats'] == {}
    assert revision['pipelines']['me/id:preview']['error_log'] == []
