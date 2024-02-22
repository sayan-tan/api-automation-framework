import requests
from Utils.json_helper import deserialize_json_to_object
from Utils.logger import log_api_error
from config import read_from_config_files


class BaseClient:
    app_settings_config = read_from_config_files()

    def __init__(self, set_auth_token):
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + set_auth_token
        }

    def get_request(self, url):
        get_url = self.app_settings_config['base_url'] + url
        get_response = requests.get(get_url, headers=self.headers)
        if not get_response.ok:
            log_api_error(get_url, None, get_response)
        return deserialize_json_to_object(get_response.text)

    def get_request_with_query_params(self, url, *args):
        get_url = self.app_settings_config['base_url'] + url
        for i in range(0, len(args)):
            get_url = get_url.replace(f'${i}', str(args[i]))
        get_response = requests.get(get_url, headers=self.headers)
        if not get_response.ok:
            log_api_error(get_url, None, get_response)
        return deserialize_json_to_object(get_response.text)

    def post_request(self, url, request):
        post_url = self.app_settings_config['base_url'] + url
        post_response = requests.post(post_url, request, headers=self.headers)
        if not post_response.ok:
            log_api_error(post_url, request, post_response)
        return deserialize_json_to_object(post_response.text)

    def put_request_with_query_params(self, url, request, *args):
        put_url = self.app_settings_config['base_url'] + url
        for i in range(0, len(args)):
            put_url = put_url.replace(f'${i}', str(args[i]))
        put_response = requests.put(put_url, request, headers=self.headers)
        if not put_response.ok:
            log_api_error(put_url, request, put_response)
        return deserialize_json_to_object(put_response.text)

    def delete_request_with_query_params(self, url, *args):
        delete_url = self.app_settings_config['base_url'] + url
        for i in range(0, len(args)):
            delete_url = delete_url.replace(f'${i}', str(args[i]))
        delete_response = requests.delete(delete_url, headers=self.headers)
        if not delete_response.ok:
            log_api_error(delete_url, None, delete_response)
        return deserialize_json_to_object(delete_response.text)

    def delete_request(self, url, request):
        delete_url = self.app_settings_config['base_url'] + url
        delete_response = requests.delete(delete_url, data=request, headers=self.headers)
        if not delete_response.ok:
            log_api_error(delete_url, None, delete_response)

