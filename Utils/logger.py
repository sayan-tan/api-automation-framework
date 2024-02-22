import logging
import os
from pathlib import Path


Log_Format = "%(levelname)s %(asctime)s - %(message)s"


def log_api_error(url, request=None, response=None):
    logging.basicConfig(filename=Path(__file__).parent.joinpath("api_errors.log"),
                        filemode="a",
                        format=Log_Format,
                        level=logging.ERROR,
                        force=True)
    logger = logging.getLogger()
    logger.error('TestName: '+os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0])
    logger.error('URL: '+url)
    if request:
        logger.error('Request: '+request)
    if response:
        logger.error('Response: '+response)
    logger.error('------END OF LOGS FOR THIS API------\n\n')
