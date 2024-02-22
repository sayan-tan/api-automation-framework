import pytest
from Data.Clients.student_details_client import StudentDetailsClient
from Utils.json_helper import read_file_with_json_extension


@pytest.mark.Smoke
@pytest.mark.jira('CUA-10')
def test_add_student_data_using_json():
    json_request = read_file_with_json_extension("add_student")
    student_client = StudentDetailsClient()
    response = student_client.add_student_details(str(json_request))
    id_of_new_student = response.id
    student = student_client.get_student_details_with_id(id_of_new_student)
    assert student.first_name == json_request['first_name']


