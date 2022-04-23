import pathlib
import shutil

import yaml


def load_pipeline_yaml(path: pathlib.Path):
    if path.is_file():
        with path.open('r') as f:
            return yaml.load(f, Loader=yaml.SafeLoader)

    raise ValueError('No Found %s' % path)

