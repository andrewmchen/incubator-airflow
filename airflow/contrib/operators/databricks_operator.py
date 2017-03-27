import logging
import time

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.databricks_hook import DatabricksHook
from airflow.models import BaseOperator

POLL_SLEEP_PERIOD_SECONDS = 5
HR = ("-" * 80)

class DatabricksSubmitRunOperator(BaseOperator):
    """
    Submits an ephemeral run to Databricks and waits
    for it to terminate successfully.
    """
    def __init__(self,
                 spark_jar_task=None,
                 notebook_task=None,
                 new_cluster=None,
                 existing_cluster_id=None,
                 libraries=[],
                 run_name=None,
                 timeout_seconds=0,
                 extra_api_parameters={},
                 databricks_conn_id='databricks_default',
                 **kwargs):
        super(DatabricksSubmitRunOperator, self).__init__(**kwargs)
        self.spark_jar_task = spark_jar_task
        self.notebook_task = notebook_task
        self.new_cluster = new_cluster
        self.existing_cluster_id = existing_cluster_id
        self.libraries = libraries
        self.run_name = task_id if run_name is None else run_name
        self.timeout_seconds = timeout_seconds
        self.extra_api_parameters = extra_api_parameters
        self.databricks_conn_id = databricks_conn_id
        self._validate_parameters()

    def _validate_parameters(self):
        self._validate_oneof(self.spark_jar_task, self.notebook_task)
        self._validate_oneof(self.new_cluster, self.existing_cluster_id)

    def _validate_oneof(self, param_a, param_b):
        if param_a is not None and param_b is None:
            pass
        elif param_a is None and param_b is not None:
            pass
        else:
            raise AirflowException

    def _log_run_page_url(self, url):
        logging.info('View run status, Spark UI, and logs at {}.'.format(url))

    def get_hook(self):
        return DatabricksHook(self.databricks_conn_id)

    def execute(self, context):
        hook = self.get_hook()
        run_id = hook.submit_run(self.spark_jar_task,
                                 self.notebook_task,
                                 self.new_cluster,
                                 self.existing_cluster_id,
                                 self.libraries,
                                 self.run_name,
                                 self.timeout_seconds,
                                 **self.extra_api_parameters)
        run_page_url = hook.get_run_page_url(run_id)
        logging.info(HR)
        logging.info('Run submitted with run_id: {}'.format(run_id))
        self._log_run_page_url(run_page_url)
        logging.info(HR)
        while True:
            run_state = hook.get_run_state(run_id)
            if run_state.is_terminal:
                if run_state.is_successful:
                    logging.info('{} completed successfully.'.format(self.task_id))
                    self._log_run_page_url(run_page_url)
                    return
                else:
                    error_message = '{t} failed with terminal state: {s}'.format(t=self.task_id,
                                                                                 s=run_state)
                    raise AirflowException(error_message)
            else:
                logging.info('{t} in run state: {s}'.format(t=self.task_id,
                                                            s=run_state))
                self._log_run_page_url(run_page_url)
                logging.info('Sleeping for {} seconds.'.format(POLL_SLEEP_PERIOD_SECONDS))
                time.sleep(POLL_SLEEP_PERIOD_SECONDS)
