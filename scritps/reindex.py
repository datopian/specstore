import os

from flowmanager.datasets import *
from flowmanager.models import FlowRegistry


DB_STRING = os.environ['FILEMANAGER_DATABASE_URL']

fr = FlowRegistry(DB_STRING)

for dataset in fr.list_datasets():
    dataset = FlowRegistry.object_as_dict(dataset)
    datahub = dataset['spec'].get('meta')
    datahub['created'] = dataset.get('created_at')
    datahub['modified'] = dataset.get('updated_at')
    descriptor = dataset['spec']['inputs'][0]['parameters'].get('descriptor', {})
    send_dataset(
        dataset.get('identifier'),
        descriptor.get('name'),
        descriptor.get('title'),
        descriptor.get('description'),
        datahub,
        descriptor,
        dataset.get('certified') or False
    )
