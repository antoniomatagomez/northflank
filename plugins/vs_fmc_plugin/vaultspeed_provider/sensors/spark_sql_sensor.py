from airflow.sdk import BaseSensorOperator
from airflow.providers.common.compat.sdk import Context


class SparkSqlSensor(BaseSensorOperator):
    """
    Sensor that executes Spark SQL queries against different types of Spark SQL connections.
    """

    template_fields = ["_sql"]

    template_ext = [".sql", ".hql"]

    def __init__(self,
                 sql,
                 spark_conn_id='spark_sql_default',
                 name=None,
                 verbose=True,
                 **kwargs):
        super().__init__(**kwargs)
        self._sql = sql
        self._conn_id = spark_conn_id
        self._name = name or self.task_id
        self._verbose = verbose
        self._hook = None

        from airflow.models.connection import Connection
        self._conn_type = Connection.get_connection_from_secrets(spark_conn_id).conn_type

    def poke(self, context: Context) -> bool:
        """Sensor poke function to get and return results from the SQL sensor."""
        if self._conn_type == 'spark_sql_vs':
            from vaultspeed_provider.hooks.spark_sql_hook import SparkSqlHook
            self._hook = SparkSqlHook(conn_id=self._conn_id,
                                      name=self._name,
                                      verbose=self._verbose
                                      )

        elif self._conn_type == 'jdbc':
            from vaultspeed_provider.hooks.jdbc_hook import JdbcHook
            self._hook = JdbcHook(jdbc_conn_id=self._conn_id)

        elif self._conn_type == 'spark_sql_livy':
            from vaultspeed_provider.hooks.livy_hook import LivyHook
            self._hook = LivyHook(conn_id=self._conn_id, task_sql_file=f"{self.dag.dag_id.lower()}/{self.task_id}.sql")

        else:
            raise Exception(f"The connection {self._conn_id} of type {self._conn_type} can not be used to execute Spark SQL.")
        
        try:
            self._hook.run(self._sql)
        except Exception as e:
            self.log.info(e)
            return False
        
        return True
