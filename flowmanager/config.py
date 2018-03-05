import os

# Auth server (to get the public key)
import datetime

auth_server = os.environ.get('AUTH_SERVER')

# Database connection string
db_connection_string = os.environ.get('DATABASE_URL')

# Datapackage Pipelines Module
dpp_module = 'assembler'

# log verbosity
verbosity = int(os.environ.get('FLOWMANAGER_VERBOSITY', 0))

# Extract values from spec
def owner_getter(spec):
    return spec.get('meta', {}).get('ownerid')


def dataset_getter(spec):
    return spec.get('meta', {}).get('dataset')


def update_time_setter(spec, now: datetime.datetime):
    spec['meta']['update_time'] = now.isoformat()
