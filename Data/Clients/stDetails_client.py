from Data.Clients.base_client import BaseClient
from config import read_from_config_files


class StDetailsClient(BaseClient):
    url_config = read_from_config_files('urls.ini', 'student_details_urls')

    def __init__(self):
        super().__init__(self.url_config['token'])

    def get_st_details_with_id(self, student_id):
        student = super().get_request_with_query_params(self.url_config['st_details_with_id'], student_id)
        return student.data

    def get_st_details(self):
        student = super().get_request(self.url_config['st_details'])
        return student.data
