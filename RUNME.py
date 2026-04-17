# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Interoperability Workshop — Setup & Orchestration
# MAGIC
# MAGIC This notebook creates a serverless Workflow to run the four workshop notebooks
# MAGIC in sequence. Attach to any compute and Run All.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Serverless compute enabled in the workspace
# MAGIC 2. Unity Catalog enabled
# MAGIC 3. `spark-xml` Maven library installed (for `file.py`)
# MAGIC 4. `dbignite` wheel installed as workspace library (for `fhir.py`)

# COMMAND ----------

import json

nb_context = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)
notebook_dir = "/".join(nb_context["extraContext"]["notebook_path"].split("/")[:-1])

# COMMAND ----------

# DBTITLE 1,Workflow Definition
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task,
    NotebookTask,
    RunIf,
)

w = WorkspaceClient()

job_name = "hls_interop_workshop"
catalog_param = {"catalog": "hls_workshop"}

tasks = [
    Task(
        task_key="jdbc_ingestion",
        notebook_task=NotebookTask(
            notebook_path=f"{notebook_dir}/jdbc",
            base_parameters=catalog_param,
        ),
        environment_key="workshop_env",
    ),
    Task(
        task_key="file_ingestion",
        notebook_task=NotebookTask(
            notebook_path=f"{notebook_dir}/file",
            base_parameters=catalog_param,
        ),
        environment_key="workshop_env",
    ),
    Task(
        task_key="fhir_to_omop",
        notebook_task=NotebookTask(
            notebook_path=f"{notebook_dir}/fhir",
            base_parameters=catalog_param,
        ),
        environment_key="workshop_env",
    ),
    Task(
        task_key="hl7_parsing",
        notebook_task=NotebookTask(
            notebook_path=f"{notebook_dir}/hl7",
            base_parameters=catalog_param,
        ),
        environment_key="workshop_env",
    ),
]

# Check if job exists
existing = [j for j in w.jobs.list(name=job_name)]
if existing:
    print(f"Job '{job_name}' already exists: {existing[0].job_id}")
else:
    job = w.jobs.create(
        name=job_name,
        tasks=tasks,
        environments=[{
            "environment_key": "workshop_env",
            "spec": {
                "client": "1",
                "dependencies": ["python-hl7"],
            },
        }],
        tags={"workshop": "hls-interop", "compute": "serverless"},
    )
    print(f"Created job '{job_name}': {job.job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Workshop
# MAGIC
# MAGIC Option A: Run the job via the Jobs UI (recommended for the workshop).
# MAGIC
# MAGIC Option B: Run each notebook individually — attach to Serverless compute,
# MAGIC set the `catalog` widget, and Run All.
# MAGIC
# MAGIC The four notebooks are independent — they can run in any order or in parallel.
# MAGIC Each creates its own schema within the target catalog.
