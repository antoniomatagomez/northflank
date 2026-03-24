from functools import cached_property

try:
    # optional import of databricks hook
    from airflow.providers.databricks.hooks.databricks import DatabricksHook
except ImportError:
    class DatabricksHook:
        def __init__(self, *args, **kwargs):
            raise ImportError("airflow.providers.databricks is required to use VSDatabricksHook")


class VSDatabricksHook(DatabricksHook):
    """
    Send VaultSpeed as the user agent for databricks connections
    """

    @cached_property
    def user_agent_value(self) -> str:
        return "VaultSpeed"
