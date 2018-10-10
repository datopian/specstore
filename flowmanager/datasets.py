import datetime
import decimal
import os

import elasticsearch
from concurrent.futures import ThreadPoolExecutor

from tableschema_elasticsearch import Storage
from tableschema_elasticsearch.mappers import MappingGenerator
from datapackage_pipelines.utilities.extended_json import LazyJsonLine

tpe = ThreadPoolExecutor(max_workers=1)

ELASTICSEARCH_HOST = os.environ.get('EVENTS_ELASTICSEARCH_HOST', 'localhost:9200')
DATASETS_INDEX_NAME = os.environ.get('DATASETS_INDEX_NAME', 'datahub')
DATASETS_DOCTYPE = 'dataset'

SCHEMA = {
    'fields': [
        {'name': 'id', 'type': 'string', 'analyzer': 'keyword'},
        {'name': 'name', 'type': 'string', 'analyzer': 'keyword'},
        {'name': 'title', 'type': 'string', 'analyzer': 'english'},
        {'name': 'description', 'type': 'string', 'analyzer': 'english'},
        {'name': 'certified', 'type': 'boolean'},
        {'name': 'datapackage', 'type': 'object', 'es:schema': {
            'fields': [
                {'name': 'readme', 'type': 'string', 'analyzer': 'english'}
            ]
        }},
        {'name': 'datahub', 'type': 'object',
         'es:schema': {
             'fields': [
                 {'name': 'owner', 'type': 'string', 'analyzer': 'english'},
                 {'name': 'ownerid', 'type': 'string', 'analyzer': 'keyword'},
                 {'name': 'findability', 'type': 'string', 'analyzer': 'keyword'},
                 {'name': 'flowid', 'type': 'string', 'analyzer': 'keyword'},
                 {'name': 'stats', 'type': 'object', 'es:schema': {
                    'fields': [
                        {'name': 'rowcount', 'type': 'integer'},
                        {'name': 'bytes', 'type': 'integer'}
                    ]}}
             ]}
         },
    ],
    'primaryKey': ['id']
}


class AnalyzerForMappingGenerator(MappingGenerator):

    @classmethod
    def _convert_type(cls, schema_type, field, prefix):
        prop = super(AnalyzerForMappingGenerator, cls)._convert_type(schema_type, field, prefix)
        analyzer = field.get('analyzer')
        if analyzer is not None:
            prop['analyzer'] = analyzer
        return prop


def _send(es: elasticsearch.Elasticsearch,
        id, name, title, description, datahub, datapackage, certified=False):
    body = {
        "id": id,
        "name": name,
        "title": title,
        "description": description,
        "datahub": datahub,
        "datapackage": datapackage,
        "certified": certified
    }

    primary_key = SCHEMA['primaryKey']
    storage = Storage(es)
    storage.create(
                DATASETS_INDEX_NAME,
                [(DATASETS_DOCTYPE, SCHEMA)],
                always_recreate=False,
                mapping_generator_cls=AnalyzerForMappingGenerator
            )

    list(storage.write(DATASETS_INDEX_NAME, DATASETS_DOCTYPE, [body],
                             primary_key, as_generator=False))

class DataSetSender():
    def __init__(self):
        self.es = elasticsearch.Elasticsearch(hosts=[ELASTICSEARCH_HOST])

    def __call__(self, *args, **kwargs):
        tpe.submit(_send, self.es, *args)

send_dataset = DataSetSender()
