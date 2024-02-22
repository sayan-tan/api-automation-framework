import json
import requests
from Utils.json_helper import deserialize_json_to_object
from config import read_from_config_files


class JiraAPI:
    jira_config = read_from_config_files('jira.ini', 'jira')
    base_url = jira_config['url']

    def get_test_id_and_execute(self, test_name, test_status):
        api_url = self.base_url + '/rest/api/2/issue/' + test_name
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': self.jira_config['authorization']
        }
        get_response = requests.get(api_url, headers=headers)
        deserialized = deserialize_json_to_object(get_response.text)
        test_id = deserialized.id
        self.get_execution_id(test_name, test_id, test_status)

    def get_execution_id(self, test_name, test_id, test_status):
        api_url = self.base_url + '/rest/zapi/latest/execution?issueId='+test_id+'&cycleId='+self.jira_config['jira_cycle_id'] +\
                  '&projectId='+self.jira_config['jira_project_id']
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': self.jira_config['authorization']
        }
        get_response = requests.get(api_url, headers=headers)
        deserialized = deserialize_json_to_object(get_response.text)
        execution_id = deserialized.executions[0].id
        self.execute_test_case(test_name, execution_id, test_status)

    def execute_test_case(self, test_name, execution_id, test_status):
        api_url = self.base_url + '/rest/zapi/latest/execution/' + str(execution_id) + '/execute'
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': self.jira_config['authorization']
        }
        if test_status == 'failed':
            api_body = {"status": "2"}
        else:
            api_body = {"status": "1"}
        put_response = requests.put(api_url, json.dumps(api_body), headers=headers)
        print('Executed '+test_name+' on Jira with status: '+test_status)
