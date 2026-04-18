# Databricks notebook source
# MAGIC %md
# MAGIC # COMPUTE: Serverless
# MAGIC
# MAGIC # Deploy Workshop Streamlit App
# MAGIC
# MAGIC Deploys the Healthcare Interoperability Workshop as a Databricks App (Streamlit).
# MAGIC The app provides a unified interface for exploring all four interoperability
# MAGIC patterns and the UC functions.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. All workshop notebooks have been run (data exists in `hls_workshop` catalog)
# MAGIC 2. UC functions registered (run `uc_functions.py`)
# MAGIC 3. A Serverless SQL Warehouse is available

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "hls_workshop", "Workshop Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy via Databricks SDK

# COMMAND ----------

# DBTITLE 1,Deploy the app
import json

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import AppDeployment

w = WorkspaceClient()

app_name = "hls-interop-workshop"

# Resolve the app directory relative to this notebook
nb_context = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)
notebook_dir = "/".join(nb_context["extraContext"]["notebook_path"].split("/")[:-1])
app_source_path = f"{notebook_dir}/app"

try:
    # Check if app already exists
    existing = w.apps.get(app_name)
    print(f"App '{app_name}' already exists. Updating...")
    deployment = w.apps.deploy(app_name, source_code_path=app_source_path)
    print(f"Deployment started: {deployment.deployment_id}")
except Exception:
    # Create new app
    try:
        app = w.apps.create(
            name=app_name,
            description="Healthcare Interoperability Workshop — explore JDBC, CCDA, FHIR/OMOP, and HL7 data",
        )
        print(f"App created: {app_name}")
        deployment = w.apps.deploy(app_name, source_code_path=app_source_path)
        print(f"Deployment started: {deployment.deployment_id}")
    except Exception as e:
        print(f"Could not deploy app: {e}")
        print()
        print("To deploy manually:")
        print(f"  1. Navigate to Compute > Apps > Create App")
        print(f"  2. Name: {app_name}")
        print(f"  3. Source: {app_source_path}")
        print(f"  4. Set env var CATALOG={catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## App Structure
# MAGIC
# MAGIC ```
# MAGIC app/
# MAGIC   app.py            — Streamlit application (6 pages)
# MAGIC   app.yaml          — Databricks App config (command, env, resources)
# MAGIC   requirements.txt  — Python dependencies
# MAGIC ```
# MAGIC
# MAGIC ### Pages
# MAGIC
# MAGIC | Page | Content |
# MAGIC |------|---------|
# MAGIC | Overview | KPI tiles + table inventory across all 4 schemas |
# MAGIC | CCDA Medications | Top medications, prescriptions by city, browsable records |
# MAGIC | FHIR / OMOP | Condition prevalence, OMOP table sizes, comorbidity distribution |
# MAGIC | HL7 Admissions | Admissions by facility, top diagnoses, filterable message table |
# MAGIC | UC Functions Explorer | Interactive code lookups (SNOMED, ICD-10, ADT), risk tier, validation |
# MAGIC | Data Quality | Null rates, SNOMED validity, CCDA completeness, audit log |
# MAGIC
# MAGIC ### Configuration
# MAGIC
# MAGIC Set the `CATALOG` environment variable to point at a different catalog:
# MAGIC
# MAGIC ```yaml
# MAGIC # In app.yaml
# MAGIC env:
# MAGIC   - name: CATALOG
# MAGIC     value: "your_catalog_name"
# MAGIC ```
# MAGIC
# MAGIC The app connects to the Serverless SQL Warehouse automatically when running
# MAGIC as a Databricks App. For local development, set `DATABRICKS_HOST` and
# MAGIC `DATABRICKS_TOKEN` environment variables.
