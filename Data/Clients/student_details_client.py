from Data.Clients.base_client import BaseClient
from config import read_from_config_files


class StudentDetailsClient(BaseClient):
    url_config = read_from_config_files('urls.ini', 'student_details_urls')

    def __init__(self):
        super().__init__(self.url_config['token'])

    def add_student_details(self, request):
        return super().post_request(self.url_config['student_details'], request)

    def get_all_student_details(self):
        return super().get_request(self.url_config['student_details'])

    def get_student_details_with_id(self, student_id):
        student = super().get_request_with_query_params(self.url_config['student_details_with_id'], student_id)
        return student.data

    def update_student_details_with_id(self, request, student_id):
        super().put_request_with_query_params(self.url_config['student_details_with_id'], request, student_id)

    def delete_student_details_with_id(self, student_id):
        super().delete_request_with_query_params(self.url_config['student_details_with_id'], student_id)
