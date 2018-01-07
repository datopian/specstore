import datetime
import decimal
import os

import elasticsearch
from concurrent.futures import ThreadPoolExecutor

from tableschema_elasticsearch import Storage
from datapackage_pipelines.utilities.extended_json import LazyJsonLine

tpe = ThreadPoolExecutor(max_workers=1)

ELASTICSEARCH_HOST = os.environ.get('EVENTS_ELASTICSEARCH_HOST', 'localhost:9200')
DATASETS_INDEX_NAME = os.environ.get('DATASETS_INDEX_NAME', 'datahub')
DATASETS_DOCTYPE = 'dataset'

SCHEMA = {
    'fields': [
        {'name': 'id', 'type': 'string'},
        {'name': 'name', 'type': 'string'},
        {'name': 'title', 'type': 'string'},
        {'name': 'description', 'type': 'string'},
        {'name': 'datapackage', 'type': 'object', 'es:schema': {
            'fields': [
                {'name': 'readme', 'type': 'string'}
            ]
        }},
        {'name': 'datahub', 'type': 'object',
         'es:schema': {
             'fields': [
                 {'name': 'owner', 'type': 'string'},
                 {'name': 'ownerid', 'type': 'string'},
                 {'name': 'findability', 'type': 'string'},
                 {'name': 'flowid', 'type': 'string'},
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


def normalize(obj):
    if isinstance(obj, (dict, LazyJsonLine)):
        return dict(
            (k, normalize(v))
            for k, v in obj.items()
        )
    elif isinstance(obj, (str, int, float, bool, datetime.date)):
        return obj
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, (list, set)):
        return [normalize(x) for x in obj]
    elif obj is None:
        return None
    assert False, "Don't know how to handle object (%s) %r" % (type(obj), obj)


def _send(es: elasticsearch.Elasticsearch,
                            id, name, title, description, datahub, datapackage):
    body = {
        "id": id,
        "name": name,
        "title": title,
        "description": description,
        "datahub": datahub,
        "datapackage": datapackage
    }

    primary_key = SCHEMA['primaryKey']
    storage = Storage(es)
    storage.create(
        DATASETS_INDEX_NAME, [(DATASETS_DOCTYPE, SCHEMA)], always_recreate=False)

    list(storage.write(DATASETS_INDEX_NAME, DATASETS_DOCTYPE, [normalize(body)],
                             primary_key, as_generator=True))

class EventSender():
    def __init__(self):
        self.es = elasticsearch.Elasticsearch(hosts=[ELASTICSEARCH_HOST])

    def __call__(self, *args, **kwargs):
        tpe.submit(_send, self.es, *args)

send_dataset = EventSender()
