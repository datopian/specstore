import datetime
import jwt
import pytest
import os

from specstore.models import FlowRegistry
from werkzeug.exceptions import NotFound
import requests_mock

from .config import load_spec

import specstore.controllers
status = specstore.controllers.status
upload = specstore.controllers.upload
update = specstore.controllers.update
info = specstore.controllers.info
get_fixed_pipeline_state = specstore.controllers.get_fixed_pipeline_state
specstore.controllers.dpp_server = 'http://dpp/'

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
        'permissions': {},
        'service': ''
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
    r.save_dataset_revision(dict(revision_id='me/id/1', dataset_id='me/id', revision=1))
    r.save_pipeline(dict(
        pipeline_id='me/id:non-tabular',
        flow_id='me/id/1',
        pipeline_details=[]))
    r.save_pipeline(dict(
        pipeline_id='me/id',
        flow_id='me/id/1',
        pipeline_details=[]))
    return r

# STATUS

def test_get_fixed_pipeline_state_not_found(empty_registry):
    with pytest.raises(NotFound):
        get_fixed_pipeline_state('me', 'id', empty_registry)


def test_get_fixed_pipeline_state_found_no_pipeline(full_registry):
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/raw/me/id', status_code=404)
        ret = get_fixed_pipeline_state('me', 'id', full_registry)
        assert ret['state'] == "LOADED"


def test_get_fixed_pipeline_state_found_has_pipeline_old_nonfinal(full_registry):
    response = {
        'state': 'RUNNING',
        'stats': {
            'hash': 'abc'
        },
        'reason': "my\nhovercraft\nis\nfull\nof\neels",
        'pipeline': {
            'update_time': (now + datetime.timedelta(seconds=-1)).isoformat()
        }
    }
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/raw/me/id', json=response)
        ret = get_fixed_pipeline_state('me', 'id', full_registry)
        response['state'] = 'REGISTERED'
        response['spec_contents'] = spec
        response['spec_modified'] = now.isoformat()
        assert ret == response


def test_get_fixed_pipeline_state_found_has_pipeline_old_final(full_registry):
    response = {
        'state': 'SUCCEEDED',
        'stats': {
            'hash': 'abc'
        },
        'reason': "my\nhovercraft\nis\nfull\nof\neels",
        'pipeline': {
            'update_time': (now + datetime.timedelta(seconds=-1)).isoformat()
        }

    }
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/raw/me/id', json=response)
        ret = get_fixed_pipeline_state('me', 'id', full_registry)
        response['state'] = 'REGISTERED'
        response['spec_contents'] = spec
        response['spec_modified'] = now.isoformat()
        assert ret == response


def test_get_fixed_pipeline_state_found_has_pipeline_current(full_registry):
    response = {
        'state': 'RUNNING',
        'stats': {
            'hash': 'abc'
        },
        'reason': "my\nhovercraft\nis\nfull\nof\neels",
        'pipeline': {
            'update_time': now.isoformat()
        }

    }
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/raw/me/id', json=response)
        ret = get_fixed_pipeline_state('me', 'id', full_registry)
        response['spec_contents'] = spec
        response['spec_modified'] = now.isoformat()
        assert ret == response


def test_status_found_has_pipeline_current(full_registry):
    response = {
        'state': 'RUNNING',
        'stats': {
            'hash': 'abc'
        },
        'reason': "my\n" * 200 + "my",
        'pipeline': {
            'update_time': now.isoformat()
        }

    }
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/raw/me/id', json=response)
        ret = status('me', 'id', full_registry)
        assert ret == {
            'state': 'RUNNING',
            'modified': response['pipeline']['update_time'],
            'logs': (["my"] * 50,),
            'stats': {
                'hash': 'abc'
            }
        }


def test_info_found_has_pipeline_current(full_registry):
    response = {
        'state': 'RUNNING',
        'stats': {
            'hash': 'abc'
        },
        'reason': "my\nhovercraft\nis\nfull\nof\neels",
        'pipeline': {
            'update_time': now.isoformat()
        }

    }
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/raw/me/id', json=response)
        ret = info('me', 'id', full_registry)
        response['spec_contents'] = spec
        response['spec_modified'] = now.isoformat()
        assert ret == response


# UPLOAD

def test_upload_no_contents(empty_registry):
    token = generate_token('me')
    ret = upload(token, None, empty_registry, public_key)
    assert not ret['success']
    assert ret['id'] is None
    assert ret['errors'] == ['Received empty contents (make sure your content-type is correct)']


def test_upload_bad_contents(empty_registry):
    token = generate_token('me')
    ret = upload(token, {}, empty_registry, public_key)
    assert not ret['success']
    assert ret['id'] is None
    assert ret['errors'] == ['Missing owner in spec']


def test_upload_no_token(empty_registry):
    ret = upload(None, spec, empty_registry, public_key)
    assert not ret['success']
    assert ret['id'] is None
    assert ret['errors'] == ['No token or token not authorised for owner']


def test_upload_bad_token(empty_registry):
    token = generate_token('mee')
    ret = upload(token, spec, empty_registry, public_key)
    assert not ret['success']
    assert ret['id'] is None
    assert ret['errors'] == ['No token or token not authorised for owner']


def test_upload_new(empty_registry: FlowRegistry):
    token = generate_token('me')
    ret = upload(token, spec, empty_registry, public_key)
    assert ret['success']
    assert ret['id'] == 'me/id'
    assert ret['errors'] == []
    specs = list(empty_registry.list_datasets())
    assert len(specs) == 1
    first = specs[0]
    assert first.owner == 'me'
    assert first.identifier == 'me/id'
    assert first.spec == spec
    revision = empty_registry.get_revision_by_dataset_id('me/id')
    assert revision['revision'] == 1
    assert revision['status'] == 'flow-pending'
    pipelines = list(empty_registry.list_pipelines('me/id/1'))
    assert len(pipelines) == 2
    pipeline = pipelines[0]
    assert pipeline.status == 'pending'


def test_upload_existing(full_registry):
    token = generate_token('me')
    ret = upload(token, spec, full_registry, public_key)
    assert ret['success']
    assert ret['id'] == 'me/id'
    assert ret['errors'] == []
    specs = list(full_registry.list_datasets())
    assert len(specs) == 1
    first = specs[0]
    assert first.owner == 'me'
    assert first.identifier == 'me/id'
    assert first.spec == spec
    revision = full_registry.get_revision_by_dataset_id('me/id')
    assert revision['revision'] == 2
    assert revision['status'] == 'flow-pending'
    pipelines = list(full_registry.list_pipelines('me/id/2'))
    assert len(pipelines) == 2
    pipeline = pipelines[0]
    assert pipeline.status == 'pending'
    ## make pipelines for previous revision are still there
    pipelines = list(full_registry.list_pipelines('me/id/1'))
    assert len(pipelines) == 2


def test_upload_append(full_registry):
    token = generate_token('me2')
    ret = upload(token, spec2, full_registry, public_key)
    assert ret['success']
    assert ret['id'] == 'me2/id2'
    assert ret['errors'] == []
    specs = list(full_registry.list_datasets())
    assert len(specs) == 2
    first = specs[1]

    assert first.owner == 'me2'
    assert first.identifier == 'me2/id2'
    assert first.spec == spec2
    second = specs[0]
    assert second.owner == 'me'
    assert second.identifier == 'me/id'
    assert second.spec == spec

    revision = full_registry.get_revision_by_dataset_id('me2/id2')
    assert revision['revision'] == 1
    assert revision['status'] == 'flow-pending'
    pipelines = list(full_registry.list_pipelines('me2/id2/1'))
    assert len(pipelines) == 2
    pipelines = list(full_registry.list_pipelines('me/id/1'))
    assert len(pipelines) == 2

def test_update_pending(full_registry):
    payload = {
      "pipeline": "me/id",
      "event": "progress",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry)
    assert ret['status'] == 'pending'
    assert ret['id'] == 'me/id/1'
    revision = full_registry.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'pending'

def test_update_fail(full_registry):
    payload = {
      "pipeline": "me/id",
      "event": "finished",
      "success": False,
      "errors": ['error']
    }
    ret = update(payload, full_registry)
    assert ret['status'] == 'failed'
    assert ret['id'] == 'me/id/1'
    revision = full_registry.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'failed'

def test_update_success(full_registry):
    payload = {
      "pipeline": "me/id",
      "event": "finished",
      "success": True,
      "errors": []
    }
    ret = update(payload, full_registry)
    assert ret['status'] == 'success'
    assert ret['id'] == 'me/id/1'
    revision = full_registry.get_revision_by_revision_id('me/id/1')
    assert revision['status'] == 'success'
    pipelines = full_registry.list_pipelines('me/id')
    assert len(list(pipelines)) == 0
