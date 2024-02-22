from pathlib import Path
import json
from types import SimpleNamespace


BASE_PATH = Path(__file__).parent.parent.joinpath('Fixtures')


def read_file_with_json_extension(file_name):
    path = get_file_with_json_extension(file_name)
    with path.open(mode='r') as f:
        return json.load(f)


def get_file_with_json_extension(file_name):
    if '.json' in file_name:
        path = BASE_PATH.joinpath(file_name)
    else:
        path = BASE_PATH.joinpath(f'{file_name}.json')
    return path


def obj_to_json(payload):
    return json.dumps(payload, default=lambda o: o.__dict__)


def json_to_dict(json_string):
    return json.loads(json_string)


def deserialize_json_to_object(json_string):
    return json.loads(json_string, object_hook=lambda d: SimpleNamespace(**d))