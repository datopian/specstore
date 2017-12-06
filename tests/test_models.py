import datetime
import unittest

from flowmanager.models import FlowRegistry

registry = FlowRegistry('sqlite://')

spec = {'meta': {'dataset': 'id', 'ownerid': 'me'}}
now = datetime.datetime.now()

class ModelsTestCase(unittest.TestCase):
    def test_format_identifier(self):
        ret = registry.format_identifier('datahub', 'dataset')
        self.assertEqual(ret, 'datahub/dataset')
        ret = registry.format_identifier('datahub', 'dataset', 1, 'pipeline')
        self.assertEqual(ret, 'datahub/dataset/1/pipeline')

    def test_save_and_get_dataset(self):
        response = dict(
            identifier='1',
            owner='datahub',
            spec=spec,
            updated_at=now,
            created_at=now,
            scheduled_for=None
        )
        registry.save_dataset(response)
        ret = registry.get_dataset('non-existing')
        self.assertIsNone(ret)
        ret = registry.get_dataset('1')
        self.assertEqual(response, ret)

    def test_create_or_update_dataset(self):
        response = dict(
            identifier='2',
            owner='datahub',
            spec=spec,
            updated_at=now,
            created_at=now,
            scheduled_for=None
        )
        registry.save_dataset(response)
        registry.create_or_update_dataset('2', 'datahub', spec, now)
        ret = registry.get_dataset('2')
        self.assertEqual(response, ret)

        registry.create_or_update_dataset('3', 'datahub', spec, now)
        ret = registry.get_dataset('3')
        self.assertEqual(ret['identifier'], '3')

    def test_save_and_get_revision(self):
        response = dict(
            revision_id='datahub/id/100',
            dataset_id='datahub/id',
            revision=1,
            created_at=now,
            updated_at=now,
            status='success',
            errors=['some not useful errors'],
            logs=['a','log','line'],
            stats={'rows':1000}
        )
        registry.save_dataset_revision(response)
        ret = registry.get_revision('non-existing')
        self.assertIsNone(ret)
        # check by dataset_id
        ret = registry.get_revision('datahub/id')
        self.assertEqual(response, ret)
        # check by revision_id
        response['revision_id'] = 'datahub/id/101'
        registry.save_dataset_revision(response)
        ret = registry.get_revision_by_revision_id('datahub/id/101')
        self.assertEqual(response, ret)

    def test_create_revision(self):
        registry.create_revision('datahub/revision', now, 'pending', [])
        ret = registry.get_revision('datahub/revision')
        self.assertEqual(ret['revision'], 1)
        registry.create_revision('datahub/revision', now, 'success', ['error'])
        ret = registry.get_revision('datahub/revision')
        self.assertEqual(ret['revision'], 2)

    def test_update_revision(self):
        registry.create_revision('datahub/update', now, 'success', [])
        ret = registry.get_revision_by_revision_id('datahub/update/1')
        self.assertEqual(ret['revision'], 1)
        self.assertEqual(ret['status'], 'success')
        registry.update_revision('datahub/update/1', dict(
            now=now, status='failed', errors=['error']))
        ret = registry.get_revision_by_revision_id('datahub/update/1')
        self.assertEqual(ret['revision'], 1)
        self.assertEqual(ret['status'], 'failed')

    def test_save_and_get_pipelines(self):
        response = dict(
            pipeline_id = 'datahub/dataset',
            flow_id = '1/datahub/id',
            pipeline_details = [],
            status = 'success',
            errors = [],
            logs = [],
            stats = {},
            updated_at = now,
            created_at = now
        )
        registry.save_pipeline(response)
        ret = registry.get_pipeline('non-existing')
        self.assertIsNone(ret)
        ret = registry.get_pipeline('datahub/dataset')
        self.assertEqual(response, ret)

    def test_update_pipeline(self):
        response = dict(
            pipeline_id = 'datahub/pipelines',
            flow_id = '2/datahub/id',
            pipeline_details = [],
            status = 'failed',
            errors = [],
            logs = [],
            stats = {},
            updated_at = now,
            created_at = now
        )
        ret = registry.get_pipeline('datahub/pipelines')
        self.assertIsNone(ret)
        registry.save_pipeline(response)
        ret = registry.get_pipeline('datahub/pipelines')
        self.assertEqual(response, ret)
        response['status'] = 'success'
        registry.update_pipeline('datahub/pipelines', response)
        ret = registry.get_pipeline('datahub/pipelines')
        self.assertEqual('success', ret['status'])
