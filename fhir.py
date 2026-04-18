# Databricks notebook source
# MAGIC %md
# MAGIC # COMPUTE: Serverless
# MAGIC
# MAGIC # FHIR to OMOP Transformation
# MAGIC
# MAGIC Transforms FHIR R4 Bundles into the OMOP Common Data Model using the `dbignite`
# MAGIC library. Then builds chronic condition feature indicators (diabetes, hypertension,
# MAGIC substance use) as a gold-layer table for analytics.
# MAGIC
# MAGIC **Prerequisite:** Install `dbignite` as a workspace library before running.
# MAGIC Upload the wheel from `alexxx-db/dbignite` `dist/dbignite-0.2.4-py3-none-any.whl`.
# MAGIC
# MAGIC **Synthetic data only — no PHI.**
# MAGIC
# MAGIC ![FHIR logo](https://www.hl7.org/fhir/assets/images/fhir-logo-www.png)

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "hls_workshop", "Target Catalog")
dbutils.widgets.text("schema", "fhir", "Target Schema")
dbutils.widgets.text("fhir_data_path", "s3://hls-eng-data-public/data/synthea/fhir/fhir/*json", "FHIR Bundle Path")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ql = f"`{catalog}`.`{schema}`"
fhir_data_path = dbutils.widgets.get("fhir_data_path")

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ql}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load FHIR Bundles
# MAGIC
# MAGIC Sample FHIR R4 bundles are available at `s3://hls-eng-data-public/data/synthea/fhir/fhir/`.
# MAGIC Alternatively, download from [Synthea](https://synthetichealth.github.io/synthea/)
# MAGIC and upload to a UC Volume.

# COMMAND ----------

# DBTITLE 1,List FHIR bundle files
files = dbutils.fs.ls(fhir_data_path.rstrip("*json").rstrip("/"))
print(f"Found {len(files)} FHIR bundle files at {fhir_data_path}")
display(spark.createDataFrame(files[:10]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transform FHIR to OMOP CDM
# MAGIC
# MAGIC The `dbignite` library's `FhirBundlesToCdm` transformer reads FHIR bundles and
# MAGIC writes four OMOP-aligned tables: `person`, `condition_occurrence`,
# MAGIC `procedure_occurrence`, and `visit_occurrence`.
# MAGIC
# MAGIC ![OMOP CDM](https://ohdsi.github.io/TheBookOfOhdsi/images/CommonDataModel/cdmDiagram.png)

# COMMAND ----------

# DBTITLE 1,FHIR to OMOP transformation
from dbignite.omop.data_model import FhirBundles, FhirBundlesToCdm, OmopCdm

fhir_model = FhirBundles(path=fhir_data_path)
cdm_model = OmopCdm(cdm_database=f"{catalog}.{schema}")

FhirBundlesToCdm().transform(fhir_model, cdm_model)

print(f"OMOP tables written to {ql}:")
for table in ["person", "condition_occurrence", "procedure_occurrence", "visit_occurrence"]:
    count = spark.sql(f"SELECT count(*) AS cnt FROM {ql}.`{table}`").first().cnt
    print(f"  {table}: {count:,} rows")

# COMMAND ----------

# DBTITLE 1,Preview conditions
display(spark.sql(f"SELECT * FROM {ql}.`condition_occurrence` LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feature Engineering: Chronic Condition Indicators
# MAGIC
# MAGIC Build per-patient binary flags for key chronic conditions. These indicators are
# MAGIC used downstream for cohort analysis and care gap identification — not for
# MAGIC predictive modeling (which is out of scope for this workshop).
# MAGIC
# MAGIC | Condition | SNOMED Codes |
# MAGIC |-----------|-------------|
# MAGIC | Diabetes | 44054006, 427089005, 127013003, 422034002 |
# MAGIC | Hypertension | 59621000 |
# MAGIC | Substance use | 55680006, 361055000, 449868002 |

# COMMAND ----------

# DBTITLE 1,Create chronic condition feature table
feature_table = f"{ql}.`condition_features`"

spark.sql(f"""
CREATE OR REPLACE TABLE {feature_table} AS
SELECT
    person_id,
    max(diabetes_ind) AS diabetes_ind,
    max(hypertension_ind) AS hypertension_ind,
    max(substance_use_ind) AS substance_use_ind
FROM (
    SELECT
        person_id,
        condition_source_value AS condition_code,
        CASE WHEN condition_source_value IN ('44054006','427089005','127013003','422034002')
             THEN 1 ELSE 0 END AS diabetes_ind,
        CASE WHEN condition_source_value IN ('59621000')
             THEN 1 ELSE 0 END AS hypertension_ind,
        CASE WHEN condition_source_value IN ('55680006','361055000','449868002')
             THEN 1 ELSE 0 END AS substance_use_ind
    FROM {ql}.`condition_occurrence`
    WHERE condition_source_value IN (
        '44054006','427089005','127013003','422034002',
        '59621000',
        '55680006','361055000','449868002'
    )
)
GROUP BY person_id
""")

print(f"Feature table created: {feature_table}")
display(spark.sql(f"SELECT * FROM {feature_table} LIMIT 20"))

# COMMAND ----------

# DBTITLE 1,Condition prevalence summary
display(spark.sql(f"""
    SELECT
        sum(diabetes_ind) AS diabetic_patients,
        sum(hypertension_ind) AS hypertensive_patients,
        sum(substance_use_ind) AS substance_use_patients,
        count(*) AS total_patients_with_flags
    FROM {feature_table}
"""))
