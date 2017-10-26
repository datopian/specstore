import os

import yaml

curdir = os.path.dirname(__file__)
specs_dir = os.path.join(curdir, 'specs')


def load_spec(spec):
    return yaml.load(open(os.path.join(specs_dir, spec+'.yaml')))
