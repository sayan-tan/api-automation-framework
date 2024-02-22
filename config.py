from configparser import ConfigParser
from pathlib import Path


def read_from_config_files(filename='app_settings.ini', section='settings'):
    # create a parser
    parser = ConfigParser()
    # read config file
    path = Path(__file__).parent.joinpath(filename)
    parser.read(path)
    # get section, default to settings
    setting = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            setting[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return setting
