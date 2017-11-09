import datetime
import logging
import time
import requests

from flowmanager.config import db_connection_string
from flowmanager.controllers import _internal_upload
from flowmanager.models import FlowRegistry

if __name__ == '__main__':
    fr = FlowRegistry(db_connection_string)
    base = datetime.datetime.now()
    now = base
    while True:
        for ds in fr.get_expired_datasets(now):
            _internal_upload(ds.owner, ds.spec, fr)
        base += datetime.timedelta(seconds=60)
        while True:
            time.sleep(5)
            now = datetime.datetime.now()
            if now > base:
                break
