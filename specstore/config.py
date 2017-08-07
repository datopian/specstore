import os

# Auth server (to get the public key)
auth_server = os.environ.get('AUTH_SERVER')

# Database connection string
db_connection_string = os.environ.get('DATABASE_URL')

# Datapackage Pipelines server (http://host:post/)
dpp_server = os.environ.get('DPP_URL')

# Datapackage Pipelines Module
dpp_module = 'assembler'


# Extract values from spec
def owner_getter(spec):
    return spec.get('meta', {}).get('ownerid')


def dataset_getter(spec):
    return spec.get('meta', {}).get('dataset')


def id_setter(spec, id):
    spec['meta']['id'] = id
