import pytest
from Data.Clients.student_details_client import StudentDetailsClient
from Data.Models.Requests.add_student_details_request import AddStudentDetails
from Data.Models.Requests.update_student_details_request import UpdateStudentDetails
from Utils.json_helper import obj_to_json


@pytest.mark.Regression
@pytest.mark.Smoke
@pytest.mark.jira('CUA-9')
def test_add_student_data_api(set_first_name, set_middle_name, set_last_name, set_dob):
    student_client = StudentDetailsClient()
    student_details = AddStudentDetails(first_name=set_first_name, middle_name=set_middle_name,
                                        last_name=set_last_name, date_of_birth=set_dob)
    request = obj_to_json(student_details)
    response = student_client.add_student_details(request)
    id_of_new_student = response.id
    student = student_client.get_student_details_with_id(id_of_new_student)
    assert student.first_name == set_first_name


@pytest.mark.Regression
@pytest.mark.jira('CUA-6')
def test_update_student_data_api():
    student_client = StudentDetailsClient()
    student_details = AddStudentDetails(first_name="John", middle_name="Hank",
                                        last_name="Doe", date_of_birth="03-01-1971")
    request = obj_to_json(student_details)
    response = student_client.add_student_details(request)
    id_of_new_student = response.id
    updated_student_details = UpdateStudentDetails(id_of_new_student, "Patrick", "Hank", "Doe", "23-09-1991")
    update_request = obj_to_json(updated_student_details)
    student_client.update_student_details_with_id(update_request, id_of_new_student)
    student = student_client.get_student_details_with_id(id_of_new_student)
    assert student.first_name == "Patrick"


@pytest.mark.Regression
@pytest.mark.jira('CUA-7')
def test_delete_student_data_api():
    student_client = StudentDetailsClient()
    student_details = AddStudentDetails(first_name="Ab", middle_name="Ra",
                                        last_name="Ham", date_of_birth="03-04-1971")
    request = obj_to_json(student_details)
    response = student_client.add_student_details(request)
    id_student = response.id
    student1 = student_client.get_student_details_with_id(id_student)
    assert student1.first_name == "Ab"
    student_client.delete_student_details_with_id(id_student)
    students = student_client.get_all_student_details()
    student = next((stu for stu in students if stu.id == id_student), None)
    assert student is None
