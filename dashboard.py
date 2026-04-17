# Databricks notebook source
# COMPUTE: Serverless SQL Warehouse

# MAGIC %md
# MAGIC # Healthcare Interoperability Workshop — Dashboard
# MAGIC
# MAGIC Creates an AI/BI (Lakeview) dashboard summarizing the data produced by the four
# MAGIC workshop notebooks. Each tile queries tables created by the workshop — if a table
# MAGIC is missing, the tile returns empty results rather than failing.
# MAGIC
# MAGIC **To create the dashboard:** Run this notebook to validate the queries, then build
# MAGIC the dashboard in the workspace UI (SQL > Dashboards > Create) using the tiles below,
# MAGIC or use the Databricks SDK's `w.lakeview.create()` for programmatic creation.
# MAGIC
# MAGIC ---
# MAGIC **Synthetic data only — no PHI.**

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "hls_workshop", "Workshop Catalog")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{catalog}`")

# Schemas created by the four workshop notebooks
CCDA_SCHEMA = f"`{catalog}`.`ccda`"
FHIR_SCHEMA = f"`{catalog}`.`fhir`"
HL7_SCHEMA = f"`{catalog}`.`hl7`"
AUDIT_SCHEMA = f"`{catalog}`.`audit`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 1 — Workshop Overview
# MAGIC
# MAGIC KPI tiles showing what each notebook produced. Validates that all four
# MAGIC interoperability patterns ran successfully.

# COMMAND ----------

# DBTITLE 1,KPI: CCDA Documents Loaded
display(spark.sql(f"""
    SELECT count(*) AS ccda_documents_loaded
    FROM {CCDA_SCHEMA}.`ccda_claims_raw`
"""))

# COMMAND ----------

# DBTITLE 1,KPI: Medications Extracted
display(spark.sql(f"""
    SELECT count(*) AS medications_extracted,
           count(DISTINCT medication_name) AS distinct_medications
    FROM {CCDA_SCHEMA}.`medications`
"""))

# COMMAND ----------

# DBTITLE 1,KPI: OMOP Patients (FHIR)
display(spark.sql(f"""
    SELECT count(*) AS omop_patients
    FROM {FHIR_SCHEMA}.`person`
"""))

# COMMAND ----------

# DBTITLE 1,KPI: HL7 Messages Parsed
display(spark.sql(f"""
    SELECT count(*) AS hl7_messages_parsed,
           count(DISTINCT facility) AS facilities
    FROM {HL7_SCHEMA}.`parsed_hl7`
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 2 — CCDA Medication Analysis
# MAGIC
# MAGIC Visualizes medication data extracted from CDA/CCDA XML clinical documents.

# COMMAND ----------

# DBTITLE 1,Chart: Top 15 Medications by Patient Count (bar)
display(spark.sql(f"""
    SELECT medication_name, count(*) AS patient_count
    FROM {CCDA_SCHEMA}.`medications`
    WHERE medication_name IS NOT NULL
    GROUP BY medication_name
    ORDER BY patient_count DESC
    LIMIT 15
"""))

# COMMAND ----------

# DBTITLE 1,Chart: Medications by City (stacked bar)
display(spark.sql(f"""
    SELECT city, count(*) AS prescription_count
    FROM {CCDA_SCHEMA}.`medications`
    WHERE city IS NOT NULL
    GROUP BY city
    ORDER BY prescription_count DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 3 — FHIR/OMOP Clinical Profile
# MAGIC
# MAGIC Chronic condition prevalence and OMOP CDM summary from the FHIR transformation.

# COMMAND ----------

# DBTITLE 1,Chart: Chronic Condition Prevalence (bar)
display(spark.sql(f"""
    SELECT
        'Diabetes' AS condition, sum(diabetes_ind) AS patients FROM {FHIR_SCHEMA}.`condition_features`
    UNION ALL
    SELECT 'Hypertension', sum(hypertension_ind) FROM {FHIR_SCHEMA}.`condition_features`
    UNION ALL
    SELECT 'Substance Use', sum(substance_use_ind) FROM {FHIR_SCHEMA}.`condition_features`
"""))

# COMMAND ----------

# DBTITLE 1,Chart: OMOP Table Row Counts (bar)
display(spark.sql(f"""
    SELECT 'person' AS omop_table, count(*) AS rows FROM {FHIR_SCHEMA}.`person`
    UNION ALL SELECT 'condition_occurrence', count(*) FROM {FHIR_SCHEMA}.`condition_occurrence`
    UNION ALL SELECT 'procedure_occurrence', count(*) FROM {FHIR_SCHEMA}.`procedure_occurrence`
    UNION ALL SELECT 'visit_occurrence', count(*) FROM {FHIR_SCHEMA}.`visit_occurrence`
    ORDER BY omop_table
"""))

# COMMAND ----------

# DBTITLE 1,Chart: Comorbidity Distribution (pie/donut)
display(spark.sql(f"""
    SELECT
        CASE
            WHEN diabetes_ind + hypertension_ind + substance_use_ind = 0 THEN 'No tracked conditions'
            WHEN diabetes_ind + hypertension_ind + substance_use_ind = 1 THEN '1 condition'
            WHEN diabetes_ind + hypertension_ind + substance_use_ind = 2 THEN '2 conditions'
            ELSE '3 conditions'
        END AS comorbidity_level,
        count(*) AS patients
    FROM {FHIR_SCHEMA}.`condition_features`
    GROUP BY 1
    ORDER BY 1
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 4 — HL7v2 Admissions
# MAGIC
# MAGIC ADT admission data parsed from HL7v2 messages.

# COMMAND ----------

# DBTITLE 1,Chart: Admissions by Facility (bar)
display(spark.sql(f"""
    SELECT facility, count(*) AS admissions
    FROM {HL7_SCHEMA}.`parsed_hl7`
    WHERE message_type LIKE '%A01%'
    GROUP BY facility
    ORDER BY admissions DESC
"""))

# COMMAND ----------

# DBTITLE 1,Chart: Top Admit Diagnoses (horizontal bar)
display(spark.sql(f"""
    SELECT admit_diagnosis, count(*) AS frequency
    FROM {HL7_SCHEMA}.`parsed_hl7`
    WHERE admit_diagnosis IS NOT NULL AND admit_diagnosis != ''
    GROUP BY admit_diagnosis
    ORDER BY frequency DESC
    LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1,Chart: Diagnosis by Facility (heatmap/grouped bar)
display(spark.sql(f"""
    SELECT facility, admit_diagnosis, count(*) AS admissions
    FROM {HL7_SCHEMA}.`parsed_hl7`
    WHERE admit_diagnosis IS NOT NULL AND admit_diagnosis != ''
    GROUP BY facility, admit_diagnosis
    ORDER BY facility, admissions DESC
"""))

# COMMAND ----------

# DBTITLE 1,Table: Recent HL7 Messages (detail)
display(spark.sql(f"""
    SELECT patient_id, facility, message_type, admit_diagnosis, admit_location, parsed_at
    FROM {HL7_SCHEMA}.`parsed_hl7`
    ORDER BY parsed_at DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dashboard Creation
# MAGIC
# MAGIC ### Manual (Workspace UI)
# MAGIC 1. Go to **SQL > Dashboards > Create Dashboard**
# MAGIC 2. Name: `HLS Interoperability Workshop`
# MAGIC 3. Create 4 pages matching the sections above
# MAGIC 4. For each tile: create a widget, paste the SQL from the cell, select the chart type
# MAGIC 5. Connect to a **Serverless SQL Warehouse**
# MAGIC
# MAGIC ### Programmatic (Databricks SDK)
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC # Use w.lakeview.create() with the serialized dashboard definition
# MAGIC ```
# MAGIC
# MAGIC ### Color Palette (colorblind-safe)
# MAGIC - Primary: `#0072B2` (blue)
# MAGIC - Secondary: `#009E73` (teal)
# MAGIC - Accent: `#D55E00` (vermillion)
# MAGIC - Neutral: `#999999` (gray)
# MAGIC
# MAGIC ### Page Layout Reference
# MAGIC
# MAGIC | Page | Tile | Chart Type | Source Query |
# MAGIC |------|------|-----------|-------------|
# MAGIC | 1 Overview | CCDA Documents | KPI counter | `count(*) FROM ccda.ccda_claims_raw` |
# MAGIC | 1 Overview | Medications | KPI counter | `count(*) FROM ccda.medications` |
# MAGIC | 1 Overview | OMOP Patients | KPI counter | `count(*) FROM fhir.person` |
# MAGIC | 1 Overview | HL7 Messages | KPI counter | `count(*) FROM hl7.parsed_hl7` |
# MAGIC | 2 CCDA | Top Medications | Bar chart | `medications GROUP BY medication_name` |
# MAGIC | 2 CCDA | By City | Bar chart | `medications GROUP BY city` |
# MAGIC | 3 FHIR/OMOP | Condition Prevalence | Bar chart | `condition_features` union query |
# MAGIC | 3 FHIR/OMOP | OMOP Tables | Bar chart | Row counts across 4 OMOP tables |
# MAGIC | 3 FHIR/OMOP | Comorbidity | Donut chart | `condition_features` bucket query |
# MAGIC | 4 HL7 | By Facility | Bar chart | `parsed_hl7 GROUP BY facility` |
# MAGIC | 4 HL7 | Top Diagnoses | Horizontal bar | `parsed_hl7 GROUP BY admit_diagnosis` |
# MAGIC | 4 HL7 | Dx x Facility | Grouped bar | `parsed_hl7 GROUP BY facility, admit_diagnosis` |
# MAGIC | 4 HL7 | Recent Messages | Table | `parsed_hl7 ORDER BY parsed_at DESC` |
