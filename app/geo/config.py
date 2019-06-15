import json
import os

import yaml as _yaml

__cwd__ = os.getcwd()
__location__ = os.path.realpath(
    os.path.join(__cwd__, os.path.dirname(__file__))
    )


def get_geo_config():

    config_file_path = os.path.join(__location__, '..', 'geo.yml')

    return _yaml.load(config_file_path)


if __name__ == '__main__':
    print(__location__)
