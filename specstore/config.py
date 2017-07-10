import os

# Auth server (to get the public key)
auth_server = os.environ.get('AUTH_SERVER')

# Database connection string
db_connection_string = os.environ.get('DATABASE_URL')

# Datapackage Pipelines Module
dpp_module = 'assembler'


# Extract values from spec
def owner_getter(spec):
    return spec.get('meta', {}).get('owner')


def id_getter(spec):
    return spec.get('meta', {}).get('id')


def id_setter(spec, id):
    spec['meta']['id'] = id
