from Data.Clients.base_client import BaseClient
from config import read_from_config_files


class UserClient(BaseClient):
    url_config = read_from_config_files('urls.ini', 'user_urls')

    def __init__(self, set_auth_token):
        super().__init__(set_auth_token)