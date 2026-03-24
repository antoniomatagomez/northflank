from collections.abc import Callable, Iterable
from typing import Any
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler

try:
    # optional import of databricks operator
    from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor
    from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
except ImportError:
    class DatabricksSqlSensor:
        def __init__(self, *args, **kwargs):
            raise ImportError("airflow.providers.databricks is required to use VSDatabricksSqlSensor")
    class DatabricksSqlHook:
        default_conn_name = "databricks_default"
        def __init__(self, *args, **kwargs):
            raise ImportError("airflow.providers.databricks is required to use VSDatabricksSqlSensor")


class VSDatabricksSqlSensor(DatabricksSqlSensor):
    """
    Adjust the Databricks SQL sensor to allow specifying the http_path as part of the connection (this is supported by the hook),
    but DatabricksSqlSensor has an additional check that blocks it.
    Also changed the default value for the catalog to None instead of empty string, since this causes the cluster config catalog setting to be overwritten.
    """

    def __init__(
        self,
        *,
        catalog: str | None = None,
        **kwargs,
    ) -> None:
        """Create DatabricksSqlSensor object using the specified input arguments."""
        super().__init__(**kwargs, catalog=catalog)

    def _get_results(self) -> bool:
        """Use the Databricks SQL hook and run the specified SQL query."""
        hook = self.hook
        sql_result = hook.run(
            self.sql,
            handler=self.handler if self.do_xcom_push else None,
        )
        self.log.debug("SQL result: %s", sql_result)
        return bool(sql_result)
