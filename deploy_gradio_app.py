# Databricks notebook source
# MAGIC %md
# MAGIC # COMPUTE: Serverless
# MAGIC
# MAGIC # Deploy Workshop Gradio App
# MAGIC
# MAGIC Deploys the Healthcare Interoperability Workshop as a Databricks App using Gradio.
# MAGIC The Gradio app provides the same 6-page interface as the Streamlit app but uses
# MAGIC Gradio's component model — useful when the workshop needs to integrate with
# MAGIC ML model demos or when Streamlit is not available.
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

# DBTITLE 1,Deploy the Gradio app
import json

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

app_name = "hls-interop-workshop-gradio"

nb_context = json.loads(
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
)
notebook_dir = "/".join(nb_context["extraContext"]["notebook_path"].split("/")[:-1])
app_source_path = f"{notebook_dir}/gradio_app"

try:
    existing = w.apps.get(app_name)
    print(f"App '{app_name}' already exists. Updating...")
    deployment = w.apps.deploy(app_name, source_code_path=app_source_path)
    print(f"Deployment started: {deployment.deployment_id}")
except Exception:
    try:
        app = w.apps.create(
            name=app_name,
            description="Healthcare Interoperability Workshop (Gradio) — JDBC, CCDA, FHIR/OMOP, HL7 data explorer",
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
# MAGIC gradio_app/
# MAGIC   app.py            — Gradio application (6 tabs)
# MAGIC   app.yaml          — Databricks App config
# MAGIC   requirements.txt  — Python dependencies
# MAGIC ```
# MAGIC
# MAGIC ### Tabs
# MAGIC
# MAGIC | Tab | Content |
# MAGIC |-----|---------|
# MAGIC | Overview | KPI metrics + table inventory |
# MAGIC | CCDA Medications | Top medications, by city, browsable records |
# MAGIC | FHIR / OMOP | Condition prevalence, OMOP sizes, enriched conditions |
# MAGIC | HL7 Admissions | Facility filter, diagnoses, message detail |
# MAGIC | UC Functions | Interactive code lookups, risk tier, validation |
# MAGIC | Data Quality | Null rates, SNOMED validity, audit log |
# MAGIC
# MAGIC ### Gradio vs. Streamlit
# MAGIC
# MAGIC Both apps (`app/` and `gradio_app/`) are functionally identical. Choose based on:
# MAGIC - **Streamlit**: simpler syntax, built-in charts, better for data dashboards
# MAGIC - **Gradio**: component-based, better for ML model demos, supports API endpoints
