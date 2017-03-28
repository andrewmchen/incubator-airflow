# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest

from airflow import __version__
from airflow.contrib.hooks.databricks_hook import DatabricksHook, RunState
from airflow.models import Connection
from airflow.utils import db

TASK_ID = 'databricks-operator'
DEFAULT_CONN_ID = 'databricks_default'
NOTEBOOK_TASK = {
    'notebook_path': '/test'
}
NEW_CLUSTER = {
    'spark_version': '2.0.x-scala2.10',
    'node_type_id': 'development-node',
    'num_workers': 1
}
RUN_ID = 1
HOST = 'databricks.com'
INVALID_HOST = 'https://databricks.com'
LOGIN = 'login'
PASSWORD = 'password'
USER_AGENT_HEADER = {'user-agent': 'airflow-{v}'.format(v=__version__)}
RUN_PAGE_URL = 'https://databricks.com/#jobs/1/runs/1'
LIFE_CYCLE_STATE = 'PENDING'
STATE_MESSAGE = 'Waiting for cluster'
GET_RUN_RESPONSE = {
    'run_page_url': RUN_PAGE_URL,
    'state': {
        'life_cycle_state': LIFE_CYCLE_STATE,
        'state_message': STATE_MESSAGE
    }
}

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


def submit_run_endpoint(host):
    return 'https://{}/api/2.0/jobs/runs/submit'.format(host)


def get_run_endpoint(host):
    return 'https://{}/api/2.0/jobs/runs/get'.format(host)


class DatabricksHookTest(unittest.TestCase):

    @db.provide_session
    def setUp(self, session=None):
        conn = session.query(Connection) \
                      .filter(Connection.conn_id == DEFAULT_CONN_ID) \
                      .first()
        conn.host = HOST
        conn.login = LOGIN
        conn.password = PASSWORD
        session.commit()

        self.hook = DatabricksHook()

    def test_parse_host_with_proper_host(self):
        host = self.hook._parse_host(HOST)
        self.assertEquals(host, HOST)

    def test_parse_host_with_invalid_host(self):
        host = self.hook._parse_host(INVALID_HOST)
        self.assertEquals(host, HOST)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_submit_run(self, mock_requests):
        mock_requests.post.return_value.json.return_value = {'run_id': '1'}

        run_id = self.hook.submit_run(notebook_task=NOTEBOOK_TASK,
                                      new_cluster=NEW_CLUSTER)

        self.assertEquals(run_id, '1')
        mock_requests.post.assert_called_once_with(submit_run_endpoint(HOST),
                                                   json={
                                                      'notebook_task': NOTEBOOK_TASK,
                                                      'new_cluster': NEW_CLUSTER,
                                                      'libraries': [],
                                                      'timeout_seconds': 0
                                                   },
                                                   auth=(LOGIN, PASSWORD),
                                                   headers=USER_AGENT_HEADER,
                                                   timeout=self.hook.timeout_seconds,
                                                   verify=False)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_get_run_page_url(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        run_page_url = self.hook.get_run_page_url(RUN_ID)

        self.assertEquals(run_page_url, RUN_PAGE_URL)
        mock_requests.get.assert_called_once_with(get_run_endpoint(HOST),
                                                  json={'run_id': RUN_ID},
                                                  auth=(LOGIN, PASSWORD),
                                                  headers=USER_AGENT_HEADER,
                                                  timeout=self.hook.timeout_seconds,
                                                  verify=False)

    @mock.patch('airflow.contrib.hooks.databricks_hook.requests')
    def test_get_run_state(self, mock_requests):
        mock_requests.get.return_value.json.return_value = GET_RUN_RESPONSE

        run_state = self.hook.get_run_state(RUN_ID)

        self.assertEquals(run_state, RunState.from_get_run_response(GET_RUN_RESPONSE))
        mock_requests.get.assert_called_once_with(get_run_endpoint(HOST),
                                                  json={'run_id': RUN_ID},
                                                  auth=(LOGIN, PASSWORD),
                                                  headers=USER_AGENT_HEADER,
                                                  timeout=self.hook.timeout_seconds,
                                                  verify=False)


class RunStateTest(unittest.TestCase):
    def test_from_get_run_response(self):
        run_state = RunState.from_get_run_response(GET_RUN_RESPONSE)
        self.assertEquals(run_state.state_message, STATE_MESSAGE)
        self.assertEquals(run_state.life_cycle_state, LIFE_CYCLE_STATE)
        self.assertEquals(run_state.result_state, None)

    def test_is_terminal(self):
        terminal_states = ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR']
        for state in terminal_states:
            run_state = RunState(state, '', '')
            self.assertTrue(run_state.is_terminal)

    def test_is_successful(self):
        run_state = RunState('TERMINATED', 'SUCCESS', '')
        self.assertTrue(run_state.is_successful)
