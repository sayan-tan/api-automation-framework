import pytest
from Data.Clients.student_details_client import StudentDetailsClient
from Data.Models.Requests.add_student_details_request import AddStudentDetails
from Utils.excel_helper import ExcelHelper
from Utils.json_helper import obj_to_json


@pytest.mark.Smoke
@pytest.mark.jira('CUA-8')
def test_add_student_data_using_excel():
    excel_helper = ExcelHelper('multiple_student_add_details.xlsx', 'Sheet1')
    total_rows = excel_helper.get_row_count()
    column_names = excel_helper.get_column_names()

    student_client = StudentDetailsClient()
    student_details = AddStudentDetails("", "", "", "")
    request = obj_to_json(student_details)
    for i in range(2, total_rows + 1):
        updated_json_request = excel_helper.update_request_with_data(i, request, column_names)
        response = student_client.add_student_details(updated_json_request)
        id_of_new_student = response.id
        student = student_client.get_student_details_with_id(id_of_new_student)
        assert student.first_name == excel_helper.get_cell_value_by_column(i, "first_name")
