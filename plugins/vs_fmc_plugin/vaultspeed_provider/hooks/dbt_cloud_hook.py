from __future__ import annotations

import json
import time
from enum import Enum
from functools import wraps, cached_property
from inspect import signature
from typing import Any, Callable, List, Sequence, Set, Tuple, Dict, TypedDict

from requests import PreparedRequest, Session
from requests.auth import AuthBase
from requests.models import Response

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpHook


def fallback_to_default_account(func: Callable) -> Callable:
    """
    Decorator which provides a fallback value for ``account_id`` or ``project_id``. If they are None or not passed
    to the decorated function, the value will be taken from the configured dbt Cloud Airflow Connection.
    """
    sig = signature(func)
    
    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable:
        bound_args = sig.bind(*args, **kwargs)
        
        # Check if account_id or project_id was not included in the function signature or, if it is, the value is not provided.
        if bound_args.arguments.get("account_id") is None:
            self = args[0]
            bound_args.arguments["account_id"] = int(self.account_id)
        
        if "project_id" in sig.parameters:
            if bound_args.arguments.get("project_id") is None:
                self = args[0]
                bound_args.arguments["project_id"] = int(self.project_id)
        
        return func(*bound_args.args, **bound_args.kwargs)
    
    return wrapper


def _get_provider_info() -> Tuple[str, str]:
    from airflow.providers_manager import ProvidersManager
    
    manager = ProvidersManager()
    package_name = "airflow-provider-vaultspeed"  # type: ignore[union-attr]
    provider = manager.providers[package_name]
    
    return package_name, provider.version


class TokenAuth(AuthBase):
    """Helper class for Auth when executing requests."""
    
    def __init__(self, token: str) -> None:
        self.token = token
    
    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        package_name, provider_version = _get_provider_info()
        request.headers["User-Agent"] = f"{package_name}-v{provider_version}"
        request.headers["Content-Type"] = "application/json"
        request.headers["Authorization"] = f"Token {self.token}"
        
        return request


class JobRunInfo(TypedDict):
    """Type class for the ``job_run_info`` dictionary."""
    
    account_id: int | None
    run_id: int


class DbtCloudJobRunStatus(Enum):
    """dbt Cloud Job statuses."""
    
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30
    TERMINAL_STATUSES = (SUCCESS, ERROR, CANCELLED)
    
    @classmethod
    def check_is_valid(cls, statuses: int | Sequence[int] | Set[int]):
        """Validates input statuses are a known value."""
        if isinstance(statuses, (Sequence, Set)):
            for status in statuses:
                cls(status)
        else:
            cls(statuses)
    
    @classmethod
    def is_terminal(cls, status: int) -> bool:
        """Checks if the input status is that of a terminal type."""
        cls.check_is_valid(statuses=status)
        
        return status in cls.TERMINAL_STATUSES.value


class DbtCloudJobRunException(AirflowException):
    """An exception that indicates a job run failed to complete."""


def get_response_data(responses: List[Response]) -> List[Dict]:
    return [data for res in responses for data in res.json()["data"]]


class DbtCloudHook(HttpHook):
    """
    Interact with dbt Cloud using the V2 API.
    """
    
    conn_name_attr = "dbt_conn_id"
    default_conn_name = "dbt_default"
    conn_type = "dbt_cloud"
    hook_name = "dbt Cloud"
    
    @staticmethod
    def get_connection_form_widgets():
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField
        
        return {
            "extra__dbt_cloud__check_interval": StringField(lazy_gettext("polling interval"), widget=BS3TextFieldWidget()),
            "extra__dbt_cloud__threads": StringField(lazy_gettext("threads"), widget=BS3TextFieldWidget())
        }
    
    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Builds custom field behavior for the dbt Cloud connection form in the Airflow UI."""
        return {
            "hidden_fields": [],
            "relabeling": {
                "login": "Account name",
                "password": "API Token",
                "schema": "Tenant",
                "host": "project name",
                "port": "environment id"
            },
            "placeholders": {
                "schema": "Defaults to 'cloud'.",
                "host": "The name of a dbt Cloud project",
                "port": "The environment id can be found by navigating to the environment in the cloud UI, and getting the last number from the url (e.g. "
                        "https://cloud.getdbt.com/next/deploy/<account_id>/projects/<project_id>/environments/<environment_id>) ",
                "extra__dbt_cloud__threads": "The maximum number of models to run in parallel in a single dbt run",
                "extra__dbt_cloud__check_interval": "How often should the job status be checked in seconds, the default is 10s"
            },
        }
    
    def __init__(self, dbt_cloud_conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(auth_type=TokenAuth, **kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        tenant = self.connection.schema if self.connection.schema else "cloud"
        self.check_interval = self.connection.extra_dejson.get("extra__dbt_cloud__check_interval") or self.connection.extra_dejson.get('check_interval') or 10
        
        self.base_url = f"https://{tenant}.getdbt.com/api/v2/accounts/"
    
    @cached_property
    def connection(self) -> Connection:
        _connection = self.get_connection(self.dbt_cloud_conn_id)
        if not _connection.password:
            raise AirflowException("An API token is required to connect to dbt Cloud.")
        
        return _connection
    
    @cached_property
    def project_id(self) -> int:
        project_name = self.connection.host
        projects = get_response_data(self.list_projects())
        matching_ids = [project["id"] for project in projects if project["name"] == project_name]
        if matching_ids:
            return matching_ids[0]
        else:
            raise AirflowException(f"The project {project_name} does not exist on this account.")
    
    @cached_property
    def account_id(self) -> int:
        account_name = self.connection.login
        accounts = self.list_accounts().json()["data"]
        matching_ids = [account["id"] for account in accounts if account["name"] == account_name]
        if matching_ids:
            return matching_ids[0]
        else:
            raise AirflowException(f"The account {account_name} is not accessible with this API Token.")
    
    def get_conn(self, *args, **kwargs) -> Session:
        session = Session()
        session.auth = self.auth_type(self.connection.password)
        
        return session
    
    def _paginate(self, endpoint: str, payload: Dict[str, Any] | None = None) -> List[Response]:
        response = self.run(endpoint=endpoint, data=payload)
        resp_json = response.json()
        limit = resp_json["extra"]["filters"]["limit"]
        num_total_results = resp_json["extra"]["pagination"]["total_count"]
        num_current_results = resp_json["extra"]["pagination"]["count"]
        results = [response]
        if num_current_results != num_total_results:
            _paginate_payload = payload.copy() if payload else {}
            _paginate_payload["offset"] = limit
            
            while not num_current_results >= num_total_results:
                response = self.run(endpoint=endpoint, data=_paginate_payload)
                resp_json = response.json()
                results.append(response)
                num_current_results += resp_json["extra"]["pagination"]["count"]
                _paginate_payload["offset"] += limit
        return results
    
    def _run_and_get_response(
      self,
      method: str = "GET",
      endpoint: str | None = None,
      payload: str | Dict[str, Any] | None = None,
      paginate: bool = False,
    ) -> Any:
        self.method = method
        
        if paginate:
            if isinstance(payload, str):
                raise ValueError("Payload cannot be a string to paginate a response.")
            
            if endpoint:
                return self._paginate(endpoint=endpoint, payload=payload)
            else:
                raise ValueError("An endpoint is needed to paginate a response.")
        
        return self.run(endpoint=endpoint, data=payload)
    
    def list_accounts(self) -> Response:
        """
        Retrieves all the dbt Cloud accounts the configured API token is authorized to access.

        :return: List of request responses.
        """
        return self._run_and_get_response()
    
    @fallback_to_default_account
    def get_account(self, account_id: int | None = None) -> Response:
        """
        Retrieves metadata for a specific dbt Cloud account.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/")
    
    @fallback_to_default_account
    def list_projects(self, account_id: int | None = None) -> List[Response]:
        """
        Retrieves metadata for all projects tied to a specified dbt Cloud account.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: List of request responses.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/projects/", paginate=True)
    
    @fallback_to_default_account
    def get_project(self, project_id: int, account_id: int | None = None) -> Response:
        """
        Retrieves metadata for a specific project.

        :param project_id: The ID of a dbt Cloud project.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/projects/{project_id}/")
    
    @fallback_to_default_account
    def list_jobs(
      self,
      account_id: int | None = None,
      order_by: str | None = None,
      project_id: int | None = None,
    ) -> List[Response]:
        """
        Retrieves metadata for all jobs tied to a specified dbt Cloud account. If a ``project_id`` is
        supplied, only jobs pertaining to this project will be retrieved.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :param order_by: Optional. Field to order the result by. Use '-' to indicate reverse order.
            For example, to use reverse order by the run ID use ``order_by=-id``.
        :param project_id: The ID of a dbt Cloud project.
        :return: List of request responses.
        """
        return self._run_and_get_response(
          endpoint=f"{account_id}/jobs/",
          payload={"order_by": order_by, "project_id": project_id},
          paginate=True,
        )
    
    @fallback_to_default_account
    def get_job(self, job_id: int, account_id: int | None = None) -> Response:
        """
        Retrieves metadata for a specific job.

        :param job_id: The ID of a dbt Cloud job.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The request response.
        """
        return self._run_and_get_response(endpoint=f"{account_id}/jobs/{job_id}")
    
    @fallback_to_default_account
    def trigger_job_run(
      self,
      job_id: int,
      cause: str,
      account_id: int | None = None,
      steps_override: List[str] | None = None,
      schema_override: str | None = None,
      additional_run_config: Dict[str, Any] | None = None,
    ) -> Response:
        """
        Triggers a run of a dbt Cloud job.

        :param job_id: The ID of a dbt Cloud job.
        :param cause: Description of the reason to trigger the job.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param steps_override: Optional. List of dbt commands to execute when triggering the job
            instead of those configured in dbt Cloud.
        :param schema_override: Optional. Override the destination schema in the configured target for this
            job.
        :param additional_run_config: Optional. Any additional parameters that should be included in the API
            request when triggering the job.
        :return: The request response.
        """
        if additional_run_config is None:
            additional_run_config = {}
        
        payload = {
            "cause": cause,
            "steps_override": steps_override,
            "schema_override": schema_override,
        }
        payload.update(additional_run_config)
        
        return self._run_and_get_response(
          method="POST",
          endpoint=f"{account_id}/jobs/{job_id}/run/",
          payload=json.dumps(payload),
        )
    
    @fallback_to_default_account
    def list_job_runs(
      self,
      account_id: int | None = None,
      include_related: List[str] | None = None,
      job_definition_id: int | None = None,
      order_by: str | None = None,
    ) -> List[Response]:
        """
        Retrieves metadata for all the dbt Cloud job runs for an account. If a ``job_definition_id`` is
        supplied, only metadata for runs of that specific job are pulled.

        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        :param job_definition_id: Optional. The dbt Cloud job ID to retrieve run metadata.
        :param order_by: Optional. Field to order the result by. Use '-' to indicate reverse order.
            For example, to use reverse order by the run ID use ``order_by=-id``.
        :return: List of request responses.
        """
        return self._run_and_get_response(
          endpoint=f"{account_id}/runs/",
          payload={
              "include_related": include_related,
              "job_definition_id": job_definition_id,
              "order_by": order_by,
          },
          paginate=True,
        )
    
    @fallback_to_default_account
    def get_job_run(
      self, run_id: int, account_id: int | None = None, include_related: List[str] | None = None
    ) -> Response:
        """
        Retrieves metadata for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param include_related: Optional. List of related fields to pull with the run.
            Valid values are "trigger", "job", "repository", and "environment".
        :return: The request response.
        """
        return self._run_and_get_response(
          endpoint=f"{account_id}/runs/{run_id}/",
          payload={"include_related": include_related},
        )
    
    def get_job_run_status(self, run_id: int, account_id: int | None = None) -> int:
        """
        Retrieves the status for a specific run of a dbt Cloud job.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :return: The status of a dbt Cloud job run.
        """
        self.log.info("Getting the status of job run %s.", str(run_id))
        
        job_run = self.get_job_run(account_id=account_id, run_id=run_id)
        job_run_status = job_run.json()["data"]["status"]
        
        self.log.info(
          "Current status of job run %s: %s", str(run_id), DbtCloudJobRunStatus(job_run_status).name
        )
        
        return job_run_status
    
    def wait_for_job_run_status(
      self,
      run_id: int,
      account_id: int | None = None,
      expected_statuses: int | Sequence[int] | Set[int] = DbtCloudJobRunStatus.SUCCESS.value,
      check_interval: int = None,
      timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Waits for a dbt Cloud job run to match an expected status.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param expected_statuses: Optional. The desired status(es) to check against a job run's current
            status. Defaults to the success status value.
        :param check_interval: Time in seconds to check on a pipeline run's status.
        :param timeout: Time in seconds to wait for a pipeline to reach a terminal status or the expected
            status.
        :return: Boolean indicating if the job run has reached the ``expected_status``.
        """
        check_interval = check_interval or self.check_interval
        
        expected_statuses = (expected_statuses,) if isinstance(expected_statuses, int) else expected_statuses
        
        DbtCloudJobRunStatus.check_is_valid(expected_statuses)
        
        job_run_info = JobRunInfo(account_id=account_id, run_id=run_id)
        job_run_status = self.get_job_run_status(**job_run_info)
        
        start_time = time.monotonic()
        
        while (
          not DbtCloudJobRunStatus.is_terminal(job_run_status) and job_run_status not in expected_statuses
        ):
            # Check if the job-run duration has exceeded the ``timeout`` configured.
            if start_time + timeout < time.monotonic():
                raise DbtCloudJobRunException(
                  f"Job run {run_id} has not reached a terminal status after {timeout} seconds."
                )
            
            # Wait to check the status of the job run based on the ``check_interval`` configured.
            time.sleep(check_interval)
            
            job_run_status = self.get_job_run_status(**job_run_info)
        
        return job_run_status in expected_statuses
    
    @fallback_to_default_account
    def cancel_job_run(self, run_id: int, account_id: int | None = None) -> None:
        """
        Cancel a specific dbt Cloud job run.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        """
        self._run_and_get_response(method="POST", endpoint=f"{account_id}/runs/{run_id}/cancel/")
    
    @fallback_to_default_account
    def list_job_run_artifacts(
      self, run_id: int, account_id: int | None = None, step: int | None = None
    ) -> List[Response]:
        """
        Retrieves a list of the available artifact files generated for a completed run of a dbt Cloud job. By
        default, this returns artifacts from the last step in the run. To list artifacts from other steps in
        the run, use the ``step`` parameter.

        :param run_id: The ID of a dbt Cloud job run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param step: Optional. The index of the Step in the Run to query for artifacts. The first step in the
            run has the index 1. If the step parameter is omitted, artifacts for the last step in the run will
            be returned.
        :return: List of request responses.
        """
        return self._run_and_get_response(
          endpoint=f"{account_id}/runs/{run_id}/artifacts/", payload={"step": step}
        )
    
    @fallback_to_default_account
    def get_job_run_artifact(
      self, run_id: int, path: str, account_id: int | None = None, step: int | None = None
    ) -> Response:
        """
        Retrieves a list of the available artifact files generated for a completed run of a dbt Cloud job. By
        default, this returns artifacts from the last step in the run. To list artifacts from other steps in
        the run, use the ``step`` parameter.

        :param run_id: The ID of a dbt Cloud job run.
        :param path: The file path related to the artifact file. Paths are rooted at the target/ directory.
            Use "manifest.json", "catalog.json", or "run_results.json" to download dbt-generated artifacts
            for the run.
        :param account_id: Optional. The ID of a dbt Cloud account.
        :param step: Optional. The index of the Step in the Run to query for artifacts. The first step in the
            run has the index 1. If the step parameter is omitted, artifacts for the last step in the run will
            be returned.
        :return: The request response.
        """
        return self._run_and_get_response(
          endpoint=f"{account_id}/runs/{run_id}/artifacts/{path}", payload={"step": step}
        )
    
    def test_connection(self) -> Tuple[bool, str]:
        """Test dbt Cloud connection."""
        try:
            self._run_and_get_response()
            return True, "Successfully connected to dbt Cloud."
        except Exception as e:
            return False, str(e)
    
    @fallback_to_default_account
    def create_job(self, job_name, command, account_id: int | None = None, project_id: int | None = None):
        payload = {
            "id": None,
            "account_id": account_id,
            "project_id": project_id,
            "name": job_name,
            "environment_id": self.connection.port,
            "execute_steps": [command],
            "state": 1,
            "dbt_version": None,
            "triggers": {
                "github_webhook": False,
                "schedule": False
            },
            "schedule": {
                "date": {
                    "type": "every_day"
                },
                "time": {
                    "type": "at_exact_hours",
                    "hours": [0]
                }
            },
            "settings": {
                "threads": int(self.connection.extra_dejson.get("extra__dbt_cloud__threads") or self.connection.extra_dejson.get('threads') or 4),
                "target_name": "VaultSpeed"
            }
        }
        
        return self._run_and_get_response(
          method="POST",
          endpoint=f"{account_id}/jobs",
          payload=json.dumps(payload)
        )
