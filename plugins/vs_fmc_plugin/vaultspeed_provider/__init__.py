__version__ = "7.0.0.0"

def get_provider_info():
    return {
        "package-name": "airflow-provider-vaultspeed",
        "name": "VaultSpeed Provider",
        "description": "A VaultSpeed provider for Apache Airflow.",
        "connection-types": [
            {"connection-type": "spark_sql_vs", "hook-class-name": "vaultspeed_provider.hooks.spark_sql_hook.SparkSqlHook"},
            {"connection-type": "spark_sql_livy", "hook-class-name": "vaultspeed_provider.hooks.livy_hook.LivyHook"},
            {"connection-type": "singlestore", "hook-class-name": "vaultspeed_provider.hooks.singlestore_hook.SingleStoreHook"},
            {"connection-type": "dbt_cli", "hook-class-name": "vaultspeed_provider.hooks.dbt_cli_hook.DbtCliHook"},
            {"connection-type": "dbt_cloud", "hook-class-name": "vaultspeed_provider.hooks.dbt_cloud_hook.DbtCloudHook"}
        ],
        "versions": [__version__],
    }
