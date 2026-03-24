from functools import cached_property

from airflow.providers.common.sql.operators.sql import SQLCheckOperator

from vaultspeed_provider.hooks.databricks_hook import VSDatabricksHook as DatabricksHook

try:
    # optional import of databricks operator
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
except ImportError:
    class DatabricksSubmitRunOperator:
        def __init__(self, *args, **kwargs):
            raise ImportError("airflow.providers.databricks is required to use VSDatabricksSubmitRunOperator")


class VSDatabricksSubmitRunOperator(DatabricksSubmitRunOperator):
    """
    Use a custom hook for databricks connections
    """

    # the caller arg was added on newer versions of the operator
    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            caller=caller
        )


class VSDatabricksSQLCheckOperator(SQLCheckOperator):
    """
    Custom SQL check operator for Databricks.
    The Default SQLCheckOperator does not work with Databricks connections since those are linked to the DatabricksHook and not the DatabricksSqlHook.
    The DatabricksHook can only interact with the API (e.g., to run notebooks), while the DatabricksSqlHook can run SQL queries on a cluster.
    """

    conn_id_field = 'databricks_conn_id'

    def __init__(
            self,
            *,
            databricks_conn_id: str,
            **kwargs,
    ) -> None:
        self.databricks_conn_id = databricks_conn_id
        super().__init__(conn_id=databricks_conn_id, **kwargs)

    @cached_property
    def _hook(self):
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
        conn_id = getattr(self, self.conn_id_field)
        self.log.debug("Get connection for %s", conn_id)
        return DatabricksSqlHook(databricks_conn_id=conn_id)
