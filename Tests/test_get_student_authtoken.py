import pytest
from Data.Clients.stDetails_client import StDetailsClient


# THIS TEST CASE IS EXPECTED TO FAIL
@pytest.mark.Regression
@pytest.mark.Smoke
@pytest.mark.jira('CUA-11')
def test_get_student_data_api_token():
    student_dt_client = StDetailsClient()
    students = student_dt_client.get_st_details_with_id(1111111111)
    assert students.count != 0
