r"""
 __     __          _ _                           _      __  ___  __   __   
 \ \   / /_ _ _   _| | |_ ___ ____   ___  ___  __| |     \ \/ _ \/ /  /_/   
  \ \ / / _` | | | | | __/ __|  _ \ / _ \/ _ \/ _` |      \/ / \ \/ /\      
   \ V / (_| | |_| | | |_\__ \ |_) |  __/  __/ (_| |      / / \/\ \/ /      
    \_/ \__,_|\__,_|_|\__|___/ .__/ \___|\___|\__,_|     /_/ \/_/\__/       
                             |_|                                            

Vaultspeed version: 6.0.0.5, generation date: 2026/03/18 13:53:39
Pipeline: CRM_SYN - Description:  - Version: 1.0.1 - Commit message:  - lock date: 2026/03/18 13:51:55
 """

from airflow import DAG
from airflow.models import Variable

from datetime import datetime, timedelta
from pathlib import Path
import json
import ast

from airflow.providers.standard.operators.empty import EmptyOperator
from vaultspeed_provider.operators.databricks_operator import VSDatabricksSubmitRunOperator


default_args = {
	"owner": "Vaultspeed"
}


path_to_mtd = Path(Variable.get("path_to_metadata"))


with open(path_to_mtd / "29_mappings_CRM_SYN_20260318_135339.json") as file: 
	mappings = json.load(file)


CRM_SYN = DAG(
	dag_id="CRM_SYN",
	default_args=default_args,
	description="",
	schedule="@daily",
	start_date=datetime.fromisoformat("2026-03-17T13:44:40+01:00"),
	catchup=False,
	max_active_tasks=3
)

start_task = EmptyOperator(
	task_id="START",
	dag=CRM_SYN
)

tasks = {"START":start_task}

for comp, info in mappings.items():
	if info["component_type"] == "operator":

		task = EmptyOperator(
			task_id=comp,
			dag=CRM_SYN,
			trigger_rule=info["trigger_rule"]
		)

	elif info["component_type"] == "custom_task_comp":

		# Process custom_parameters: apply ast.literal_eval() on values, then resolve callables
		custom_params = {}
		for key, value in info.get("custom_parameters", {}).items():
			if isinstance(value, str):
				# First try ast.literal_eval() for literals (booleans, numbers, lists, dicts, etc.)
				try:
					value = ast.literal_eval(value)
				except (ValueError, SyntaxError):
					# If literal_eval fails, try to resolve as callable from globals
					if value in globals():
						obj = globals()[value]
						if callable(obj):
							value = obj
					# Otherwise keep as string
			custom_params[key] = value
	                             
		task = globals()[info["airflow_operator"]](
			task_id=comp,
			dag=CRM_SYN,
			trigger_rule=info["trigger_rule"],
			**custom_params
		)

	else:

		task = VSDatabricksSubmitRunOperator(
		task_id=comp,
		databricks_conn_id="flow_dbricks",
		notebook_task={
			"notebook_path": f"""{{{{var.value.databricks_path}}}}FLOW_STUDIO_MAPPINGS/Domains/CRM_Dom/{info["original_name"]}{{{{var.value.databricks_file_extension}}}}"""
		},
		existing_cluster_id="{{ conn.flow_dbricks.extra_dejson.get('cluster_id') }}",
			trigger_rule=info["trigger_rule"],
		dag=CRM_SYN
)


	tasks[comp] = task


# Set up dependencies in a second pass
for comp, info in mappings.items():
	for dep in info["dependencies"]:
		tasks[comp] << tasks[dep]


