import datetime
import jwt
import pytest
from datapackage_pipelines_sourcespec_registry.registry import SourceSpecRegistry
from werkzeug.exceptions import NotFound
import requests_mock

import specstore.controllers
status = specstore.controllers.status
upload = specstore.controllers.upload
info = specstore.controllers.info
get_fixed_pipeline_state = specstore.controllers.get_fixed_pipeline_state
specstore.controllers.dpp_server = 'http://dpp/'

private_key = open('tests/private.pem').read()
public_key = open('tests/public.pem').read()
spec = {'meta': {'dataset': 'id', 'ownerid': 'me'}}
spec2 = {'meta': {'dataset': 'id2', 'ownerid': 'me2'}}
spec_unauth = {'meta': {'dataset': 'id', 'ownerid': 'me2'}}
bad_spec = {'meta': {'dataset': 'id', 'ownerid': 'me', 'version': 'one'}}

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
    r = SourceSpecRegistry('sqlite://')
    return r


@pytest.fixture
def full_registry():
    r = SourceSpecRegistry('sqlite://')
    r.put_source_spec('id', 'me', 'assembler', spec, now=now)
    return r

# STATUS

def test_get_fixed_pipeline_state_not_found(empty_registry):
    with pytest.raises(NotFound):
        get_fixed_pipeline_state('me', 'id', empty_registry)


def test_get_fixed_pipeline_state_found_no_pipeline(full_registry):
    with requests_mock.Mocker() as mock:
        mock.get('http://dpp/api/raw/me/id', status_code=404)
        ret = get_fixed_pipeline_state('me', 'id', full_registry)
        assert ret == {
            "state": "LOADED"
        }


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
            'logs': ["my"] * 50,
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


def test_upload_invalid_contents(empty_registry):
    token = generate_token('me')
    ret = upload(token, bad_spec, empty_registry, public_key)
    assert not ret['success']
    assert ret['id'] is None
    assert ret['errors'] == ['Validation failed for contents']


def test_upload_new(empty_registry: SourceSpecRegistry):
    token = generate_token('me')
    ret = upload(token, spec, empty_registry, public_key)
    assert ret['success']
    assert ret['id'] == 'me/id'
    assert ret['errors'] == []
    specs = list(empty_registry.list_source_specs())
    assert len(specs) == 1
    first = specs[0]
    assert first.owner == 'me'
    assert first.uid == 'me/id'
    assert first.contents == spec


def test_upload_existing(full_registry):
    token = generate_token('me')
    ret = upload(token, spec, full_registry, public_key)
    assert ret['success']
    assert ret['id'] == 'me/id'
    assert ret['errors'] == []
    specs = list(full_registry.list_source_specs())
    assert len(specs) == 1
    first = specs[0]
    assert first.owner == 'me'
    assert first.uid == 'me/id'
    assert first.contents == spec


def test_upload_append(full_registry):
    token = generate_token('me2')
    ret = upload(token, spec2, full_registry, public_key)
    assert ret['success']
    assert ret['id'] == 'me2/id2'
    assert ret['errors'] == []
    specs = list(full_registry.list_source_specs())
    assert len(specs) == 2
    first = specs[0]
    assert first.owner == 'me'
    assert first.uid == 'me/id'
    assert first.contents == spec
    second = specs[1]
    assert second.owner == 'me2'
    assert second.uid == 'me2/id2'
    assert second.contents == spec2
