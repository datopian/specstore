import datetime
import unittest

from specstore.models import FlowRegistry

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
            updated_at=now
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
            updated_at=now
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
            status='ok',
            errors=['some not useful errors']
        )
        registry.save_dataset_revision(response)
        ret = registry.get_revision_by_dataset_id('non-existing')
        self.assertIsNone(ret)
        # check by dataset_id
        ret = registry.get_revision_by_dataset_id('datahub/id')
        self.assertEqual(response, ret)
        # check by revision_id
        response['revision_id'] = 'datahub/id/101'
        registry.save_dataset_revision(response)
        ret = registry.get_revision_by_revision_id('datahub/id/101')
        self.assertEqual(response, ret)

    def test_create_revision(self):
        registry.create_revision('datahub/revision', now, 'ok', [])
        ret = registry.get_revision_by_dataset_id('datahub/revision')
        self.assertEqual(ret['revision'], 1)
        registry.create_revision('datahub/revision', now, 'Not OK', ['error'])
        ret = registry.get_revision_by_dataset_id('datahub/revision')
        self.assertEqual(ret['revision'], 2)

    def test_update_revision(self):
        registry.create_revision('datahub/update', now, 'ok', [])
        ret = registry.get_revision_by_revision_id('datahub/update/1')
        self.assertEqual(ret['revision'], 1)
        self.assertEqual(ret['status'], 'ok')
        registry.update_revision('datahub/update/1', dict(
            now=now, status='Not OK', errors=['error']))
        ret = registry.get_revision_by_revision_id('datahub/update/1')
        self.assertEqual(ret['revision'], 1)
        self.assertEqual(ret['status'], 'Not OK')

    def test_save_and_get_pipelines(self):
        response = dict(
            pipeline_id = 'datahub/dataset',
            flow_id = '1/datahub/id',
            pipeline_details = [],
            status = 'ok',
            errors = [],
            updated_at = now
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
            status = 'ok',
            errors = [],
            updated_at = now
        )
        ret = registry.get_pipeline('datahub/pipelines')
        self.assertIsNone(ret)
        registry.save_pipeline(response)
        ret = registry.get_pipeline('datahub/pipelines')
        self.assertEqual(response, ret)
        response['status'] = 'not-ok'
        registry.update_pipeline('datahub/pipelines', response)
        ret = registry.get_pipeline('datahub/pipelines')
        self.assertEqual('not-ok', ret['status'])
