import io
from typing import Any

import requests
import os

from airflow.providers.common.compat.sdk import BaseHook
from requests.auth import HTTPBasicAuth


class LivyHook(BaseHook):
	"""
	This hook is a wrapper around the Apache Livy API for Spark.
	
	:param conn_id: connection_id string
	:type conn_id: str
	"""
	
	conn_name_attr = 'spark_conn_id'
	default_conn_name = 'spark_sql_default'
	conn_type = 'spark_sql_livy'
	hook_name = 'Spark Livy VaultSpeed'

	@classmethod
	def get_connection_form_widgets(cls) -> dict[str, Any]:
		"""Returns connection widgets to add to connection form"""
		from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
		from flask_babel import lazy_gettext
		from wtforms import StringField

		return {
			"hdfs_host": StringField(lazy_gettext('HDFS Host'), widget=BS3TextFieldWidget()),
			"base_hdfs_path": StringField(lazy_gettext('HDFS base Path'), widget=BS3TextFieldWidget()),
			"driver_cores": StringField(lazy_gettext('Driver Cores'), widget=BS3TextFieldWidget()),
			"driver_memory": StringField(lazy_gettext('Driver Memory'), widget=BS3TextFieldWidget()),
			"executor_cores": StringField(lazy_gettext('Executor Cores'), widget=BS3TextFieldWidget()),
			"executor_memory": StringField(lazy_gettext('Executor Memory'), widget=BS3TextFieldWidget()),
			"num_executors": StringField(lazy_gettext('Number of Executors'), widget=BS3TextFieldWidget()),
			"queue": StringField(lazy_gettext('Queue'), widget=BS3TextFieldWidget()),
			"spark_conf": StringField(lazy_gettext('Spark Config'), widget=BS3TextFieldWidget())
		}

	@classmethod
	def get_ui_field_behaviour(cls) -> dict[str, Any]:
		"""Returns custom field behaviour"""
		return {
			"hidden_fields": ['port', 'schema'],
			"relabeling": {},
			"placeholders": {
				'host': 'url of the livy API',
				'login': 'user name',
				'password': 'password'
			},
		}
	
	def __init__(self, task_sql_file, conn_id='spark_sql_default'):
		super(LivyHook, self).__init__()
		self.conn_id = conn_id
		
		_conn = self.get_connection(self.conn_id)
		self._host = _conn.host
		self._login = _conn.login
		self._password = _conn.password

		self._hdfs_host = _conn.extra_dejson.get("hdfs_host")
		self._base_hdfs_path = _conn.extra_dejson.get("base_hdfs_path")
		
		self._driver_cores = _conn.extra_dejson.get("driver_cores")
		self._driver_memory = _conn.extra_dejson.get("driver_memory")
		self._executor_cores = _conn.extra_dejson.get("executor_cores")
		self._executor_memory = _conn.extra_dejson.get("executor_memory")
		self._num_executors = _conn.extra_dejson.get("num_executors")
		self._queue = _conn.extra_dejson.get("queue")
		self._spark_conf = _conn.extra_dejson.get("spark_conf")

		self._task_sql_file = task_sql_file

	def run(self, sql: str):
		if isinstance(sql, list):
			sql = ";\n".join(sql)
		hdfs_queries_path = self.upload_sql_hdfs(sql)
		self.run_batch(hdfs_queries_path)
		self.clean_up_sql_folders(hdfs_queries_path)

	def upload_sql_hdfs(self, sql: str):
		file_remote_path = os.path.join(self._base_hdfs_path, self._task_sql_file)
		self.log.info(f"pushing SQL file to {file_remote_path}")
		with io.StringIO(sql) as sql_file:
			response = requests.put(
				f"{self._hdfs_host}{file_remote_path}?user.name={self._login}&op=CREATE&overwrite=True",
				headers={'content-type': 'application/octet-stream'}, data=sql_file,
				auth=HTTPBasicAuth(self._login, self._password),
			)
			if response.ok:
				return file_remote_path
		raise Exception(f"Failed to upload sql file to hdfs {response.text} with status code {response.status_code}")

	def clean_up_sql_folders(self, hdfs_queries_path):
		self.log.info(f"Deleting SQL file from HDFS {hdfs_queries_path}")
		response = requests.delete(
			f"{self._hdfs_host}{hdfs_queries_path}?user.name={self._login}&op=DELETE&recursive=True",
			auth=HTTPBasicAuth(self._login, self._password),
		)
		if response.ok:
			self.log.info(f"Deleted SQL file from HDFS {hdfs_queries_path}")
		else:
			self.log.error(f"Failed to delete SQL file from HDFS {response.text}")

	def kill(self):
		self.log.info(f"Killing batch {self._batch.batch_id}")
		self._batch.kill()

	def run_batch(self, hdfs_sql_queries):
		from livy import LivyBatch, SessionState
		self.log.info("Starting Livy batch")
		self._batch = LivyBatch.create(
			url=self._host,
			py_files=None,
			auth=HTTPBasicAuth(self._login, self._password),
			file=f"/user/{self._login}/airflow_batch/vault_dags_batch.py",
			files=[hdfs_sql_queries],
			driver_memory=self._driver_memory,
			driver_cores=self._driver_cores,
			executor_cores=self._executor_cores,
			executor_memory=self._executor_memory,
			num_executors=self._num_executors,
			queue=self._queue,
			spark_conf=self._spark_conf,
		)
		self.log.info(f"Batch application submitted, batch_id: {self._batch.batch_id}")
		self._batch.wait()
		if self._batch.state != SessionState.SUCCESS:
			self.log.info(self._batch.log())
			raise Exception(f"Batch {self._batch.batch_id} failed with state {self._batch.state}")
		self.log.info(f"Batch completed successfully for batch {self._batch.batch_id}")


"""
In order to use this Livy implementation, a file 'airflow_batch/vault_dags_batch.py' has to be created in the user directory 
with the following code:

*******************************************************************************************************************************
import os
import re
from logging import getLogger, Formatter

from pyspark.sql import SparkSession

log = getLogger(__name__)
formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

spark = (
    SparkSession
    .builder
    .master("yarn")
    .appName("livy-batch-airflow")
    .getOrCreate()
)

sql_files = [file for file in os.listdir('.') if file.endswith(".sql")]
for sql_file in sql_files:
    sql_file_path = os.path.join((os.getcwd()), sql_file)
    with open(os.path.join(os.getcwd(), sql_file), "r") as file:
        multiple_sql_query = file.read()
        log.info(f"Running sql file {sql_file_path}")
    for sql_query in re.split(r";\s*\\n", multiple_sql_query.strip()):
        formatted_query = sql_query.strip()
        if formatted_query:
            spark.sql(formatted_query).show()
*******************************************************************************************************************************
"""