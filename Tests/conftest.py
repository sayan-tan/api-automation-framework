import os
import pytest
import requests
from Utils.jira_api import JiraAPI
from config import read_from_config_files
from Utils.json_helper import deserialize_json_to_object

app_settings_config = read_from_config_files()
url_config = read_from_config_files('urls.ini', 'user_urls')

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """This wrapper gets called before and after every test and is used to track the test results in Jira through the Jira API."""
    app_settings_config = read_from_config_files()
    outcome = yield
    if app_settings_config['run_jira'] == 'true':
        rep = outcome.get_result()
        if rep.when == "call":
            print('teardown')
            test_status = 'failed' if rep.failed else 'passed'
            jira_api_object = JiraAPI()
            jira_id = None
            for marker in item.iter_markers(name="jira"):
                jira_id = marker.args[0]
            jira_api_object.get_test_id_and_execute(jira_id, test_status)

@pytest.fixture(scope="session")
def set_auth_token():
    token_url = app_settings_config['base_url'] + url_config['generate_token']
    auth_data = {
        'grant_type': 'password',
        'userName': f"{app_settings_config['username']}",
        'password': f"{app_settings_config['password']}"
    }
    response = requests.post(token_url, auth_data)
    token_response = deserialize_json_to_object(response.text)
    return token_response.token


def pytest_sessionstart(session):
    token_url = app_settings_config['base_url'] + url_config['register_user']
    auth_data = {
        'userName': f"{app_settings_config['username']}",
        'password': f"{app_settings_config['password']}"
    }
    response = requests.post(token_url, auth_data)
    print(response)
    
@pytest.fixture
def set_first_name():
    """Using fixture to set values in tests"""
    return "Neil"

@pytest.fixture
def set_middle_name():
    """Using fixture to set values in tests"""
    return "Nitin"

@pytest.fixture
def set_last_name():
    """Using fixture to set values in tests"""
    return "Mukesh"

@pytest.fixture
def set_dob():
    """Using fixture to set values in tests"""
    return "23-09-1999"            