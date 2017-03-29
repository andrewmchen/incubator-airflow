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

import logging
import requests

from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

from requests.exceptions import ConnectionError, Timeout

try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse


SUBMIT_RUN_ENDPOINT = ('POST', 'api/2.0/jobs/runs/submit')
GET_RUN_ENDPOINT = ('GET', 'api/2.0/jobs/runs/get')
USER_AGENT_HEADER = {'user-agent': 'airflow-{v}'.format(v=__version__)}


class DatabricksHook(BaseHook):
    """
    Interact with Databricks.
    """
    def __init__(
            self,
            databricks_conn_id='databricks_default',
            timeout_seconds=60,
            retry_limit=3):
        self.databricks_conn_id = databricks_conn_id
        self.databricks_conn = self.get_connection(databricks_conn_id)
        self.timeout_seconds = timeout_seconds
        self.retry_limit = retry_limit

    def _parse_host(self, host):
        """
        The purpose of this function is to tolerate improper connections
        where users supply ``https://databricks.com`` instead of the
        correct ``databricks.com`` for the host.
        """
        urlparse_host = urlparse.urlparse(host).hostname
        if urlparse_host:
            # In this case, host = https://databricks.com
            return urlparse_host
        else:
            # In this case, host = databricks.com
            return host

    def _do_api_call(self, endpoint_info, api_parameters):
        method, endpoint = endpoint_info
        url = 'https://{host}/{endpoint}'.format(
            host=self._parse_host(self.databricks_conn.host),
            endpoint=endpoint)
        auth = (self.databricks_conn.login, self.databricks_conn.password)
        if method == 'GET':
            request_func = requests.get
        elif method == 'POST':
            request_func = requests.post
        else:
            raise AirflowException('Unexpected HTTP Method: ' + method)

        for attempt_num in range(1, self.retry_limit+1):
            try:
                response = request_func(
                    url,
                    json=api_parameters,
                    auth=auth,
                    headers=USER_AGENT_HEADER,
                    timeout=self.timeout_seconds,
                    verify=False)
                if response.status_code == 200:
                    return response.json()
                else:
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    raise AirflowException(response.content)
            except (ConnectionError, Timeout) as e:
                logging.error(('Attempt {0} API Request to Databricks failed ' +
                              'with reason: {1}').format(attempt_num, e))
        raise AirflowException(('API requests to Databricks failed {} times. ' +
                               'Giving up.').format(self.retry_limit))

    def submit_run(
            self,
            spark_jar_task=None,
            notebook_task=None,
            new_cluster=None,
            existing_cluster_id=None,
            libraries=[],
            run_name=None,
            timeout_seconds=0,
            **kwargs):
        api_params = kwargs.copy()
        if spark_jar_task is not None:
            api_params['spark_jar_task'] = spark_jar_task
        if notebook_task is not None:
            api_params['notebook_task'] = notebook_task
        if new_cluster is not None:
            api_params['new_cluster'] = new_cluster
        if existing_cluster_id is not None:
            api_params['existing_cluster_id'] = existing_cluster_id
        api_params['libraries'] = libraries
        if run_name is not None:
            api_params['run_name'] = run_name
        api_params['timeout_seconds'] = timeout_seconds
        response = self._do_api_call(SUBMIT_RUN_ENDPOINT, api_params)
        return response['run_id']

    def get_run_page_url(self, run_id):
        api_params = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, api_params)
        return response['run_page_url']

    def get_run_state(self, run_id):
        api_params = {'run_id': run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, api_params)
        return RunState.from_get_run_response(response)


class RunState:
    def __init__(self, life_cycle_state, result_state, state_message):
        self.life_cycle_state = life_cycle_state
        self.result_state = result_state
        self.state_message = state_message

    @classmethod
    def from_get_run_response(cls, response):
        life_cycle_state = response['state']['life_cycle_state']
        # result_state may not be in the state if not terminal
        result_state = response['state'].get('result_state', None)
        state_message = response['state']['state_message']

        return cls(life_cycle_state, result_state, state_message)

    @property
    def is_terminal(self):
        return self.life_cycle_state == 'TERMINATED' or \
               self.life_cycle_state == 'SKIPPED' or \
               self.life_cycle_state == 'INTERNAL_ERROR'

    @property
    def is_successful(self):
        return self.result_state == 'SUCCESS'

    def __eq__(self, other):
        return self.life_cycle_state == other.life_cycle_state and \
               self.result_state == other.result_state and \
               self.state_message == other.state_message

    def __repr__(self):
        return str(self.__dict__)
