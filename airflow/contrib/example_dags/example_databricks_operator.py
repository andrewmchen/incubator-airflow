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

import airflow

from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime, timedelta


args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example_databricks_operator', default_args=args,
    schedule_interval='@daily')

NEW_CLUSTER_SPEC = {
    'spark_version': '2.1.0-db3-scala2.11',
    'node_type_id': 'r3.xlarge',
    'aws_attributes': {
        'availability': 'ON_DEMAND'
    },
    'num_workers': 8
}

notebook_task_params = {
    'new_cluster': NEW_CLUSTER_SPEC,
    'notebook_task': {
        'notebook_path': '/test',
    },
}
notebook_task = DatabricksSubmitRunOperator(
    task_id='notebook_task',
    dag=dag,
    **notebook_task_params)

spark_jar_task_params = {
    'new_cluster': NEW_CLUSTER_SPEC,
    'spark_jar_task': {
        'main_class_name': 'com.databricks.ComputeModels'
    },
    'libraries': [
        {
            'jar': 'dbfs:/my-jar.jar'
        },
    ]
}
spark_jar_task = DatabricksSubmitRunOperator(
    task_id='spark_jar_task',
    dag=dag,
    **spark_jar_task_params)

notebook_task.set_downstream(spark_jar_task)
