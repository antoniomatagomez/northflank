from airflow.utils.context import Context

try:
    # optional import of Google operator
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
except ImportError:
    class BigQueryInsertJobOperator:
        def __init__(self, *args, **kwargs):
            raise ImportError("airflow.providers.google is required to use VSBigQueryExecuteQueryOperator")


class VSBigQueryOperator(BigQueryInsertJobOperator):
    """
    Use a custom hook for Bigquery connections
    """
    
    def execute(self, context: Context):
        from vaultspeed_provider.hooks.bigquery_hook import VSBigqueryHook
        from airflow.providers.google.cloud.hooks import bigquery as bq_module

        # Monkey patch the original hook with the custom version.
        # This is needed because BigQueryInsertJobOperator does not use a hook factory method,
        #   and the execute method no longer checks for the presence of self.hook

        # Save the original and replace it
        original_hook = bq_module.BigQueryHook
        bq_module.BigQueryHook = VSBigqueryHook

        try:
            return super().execute(context)
        finally:
            # Always restore the original, even if execute() raises an exception
            bq_module.BigQueryHook = original_hook
