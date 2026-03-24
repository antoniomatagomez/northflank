import json
import shutil
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.standard.hooks.subprocess import SubprocessHook


class DbtCliHook(SubprocessHook):
    """
    Run commands in the dbt CLI
    """

    conn_name_attr = "dbt_conn_id"
    default_conn_name = "dbt_default"
    conn_type = "dbt_cli"
    hook_name = "dbt CLI"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["port", "extra", "password"],
            "relabeling": {"schema": "CLI flags", "host": "Path to dbt project", "login": "dbt binary"},
            "placeholders": {
                "host": "Path to the dbt project directory.",
                "login": "path of the dbt CLI binary, the default is 'dbt' which requires it to be in the PATH.",
                "schema": "Extra CLI flags.",
            },
        }

    def __init__(self, dbt_conn_id="dbt_default"):
        super().__init__()
        self.dbt_conn_id = dbt_conn_id

        conn = self.get_connection(self.dbt_conn_id)

        self.path = conn.host
        self.bin = conn.login or "dbt"
        self.flags = conn.schema
        
    def run_cli(self, selectors, variables):
        command = f"{self.bin} run --select {','.join(selectors)}"
        if variables:
            command += f" --vars \"{json.dumps(variables)}\""
        if self.flags:
            command += " " + self.flags

        result = self.run_command(command=[shutil.which("bash") or "bash", "-c", command], cwd=self.path)

        if result.exit_code != 0:
            raise AirflowException(
              f"Dbt command failed. The command returned a non-zero exit code {result.exit_code}."
            )

    def test_connection(self):
        try:
            command = f"{self.bin} --version"
            result = self.run_command(command=[shutil.which("bash") or "bash", "-c", command], cwd=self.path)
            if result.exit_code != 0:
                raise AirflowException(
                    f"Dbt command failed. The command returned a non-zero exit code {result.exit_code}."
                )
            return True, f"Connection successful, the dbt version is: {result.output}"
        except Exception as e:
            return False, f"Connection test failed: {str(e)}"