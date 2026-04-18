# Databricks notebook source
# MAGIC %md
# MAGIC # COMPUTE: Serverless
# MAGIC
# MAGIC # File-Based Ingestion: CDA/CCDA XML Documents
# MAGIC
# MAGIC Demonstrates loading clinical XML documents (CDA/CCDA format) from a Unity Catalog
# MAGIC Volume, parsing nested XML structures, and extracting structured medication data.
# MAGIC
# MAGIC **Prerequisite:** Install `com.databricks:spark-xml_2.12:0.17.0` as a workspace
# MAGIC library (Maven coordinates) before running this notebook.
# MAGIC
# MAGIC **Synthetic data only — no PHI.**

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "hls_workshop", "Target Catalog")
dbutils.widgets.text("schema", "ccda", "Target Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ql = f"`{catalog}`.`{schema}`"

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ql}")

# Volume for raw clinical documents
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {ql}.`raw_files`
COMMENT 'Landing zone for CDA/CCDA XML clinical documents'
""")

volume_path = f"/Volumes/{catalog}/{schema}/raw_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download Synthetic CCDA Data to Volume
# MAGIC
# MAGIC Synthea provides sample CCDA files. We download them into a UC Volume
# MAGIC rather than DBFS — Volumes are the UC-native file storage for Serverless.

# COMMAND ----------

# DBTITLE 1,Download Synthea CCDA sample to Volume
import urllib.request
import zipfile
import os

ccda_url = "https://synthetichealth.github.io/synthea-sample-data/downloads/synthea_sample_data_ccda_sep2019.zip"
zip_path = f"{volume_path}/synthea_ccda.zip"
extract_dir = f"{volume_path}/ccda"

try:
    existing = dbutils.fs.ls(extract_dir)
    print(f"CCDA files already present ({len(existing)} files). Skipping download.")
except Exception:
    print("Downloading Synthea CCDA sample data...")
    # Download zip to Volume via dbutils
    local_tmp = "/tmp/synthea_ccda.zip"
    urllib.request.urlretrieve(ccda_url, local_tmp)
    dbutils.fs.cp(f"file:{local_tmp}", zip_path)

    # Extract — read zip from local, write extracted files to Volume
    dbutils.fs.mkdirs(extract_dir)
    with zipfile.ZipFile(local_tmp, "r") as z:
        for name in z.namelist():
            if name.endswith(".xml"):
                data = z.read(name)
                # Write each XML file to the Volume
                local_xml = f"/tmp/{os.path.basename(name)}"
                with open(local_xml, "wb") as f:
                    f.write(data)
                dbutils.fs.cp(f"file:{local_xml}", f"{extract_dir}/{os.path.basename(name)}")

    extracted = dbutils.fs.ls(extract_dir)
    print(f"Extracted {len(extracted)} CCDA XML files to {extract_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Batch Load XML Files
# MAGIC
# MAGIC The `spark-xml` library reads XML with a specified row tag. For CCDA documents,
# MAGIC the root element is `ClinicalDocument`. Case sensitivity is required for nested
# MAGIC field access in CCDA's mixed-case element names.

# COMMAND ----------

# DBTITLE 1,Load CCDA XML documents
# Case sensitivity required for CCDA's mixed-case XML element names
spark.conf.set("spark.sql.caseSensitive", "true")

df = (
    spark.read.format("xml")
    .option("rowTag", "ClinicalDocument")
    .load(extract_dir)
)

raw_table = f"{ql}.`ccda_claims_raw`"
df.write.format("delta").mode("overwrite").saveAsTable(raw_table)

print(f"Loaded {df.count()} CCDA documents to {raw_table}")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extract Medication Data
# MAGIC
# MAGIC CCDA documents contain deeply nested XML structures. The medication section
# MAGIC is buried inside `component > structuredBody > component > section` where
# MAGIC `section.title = 'Medications'`. We explode the nested arrays to flatten
# MAGIC each medication into a queryable row.

# COMMAND ----------

# DBTITLE 1,Create medications table
med_table = f"{ql}.`medications`"
spark.sql(f"DROP TABLE IF EXISTS {med_table}")

spark.sql(f"""
CREATE TABLE {med_table} AS
SELECT DISTINCT
    recordTarget.patientRole.addr.city AS city,
    recordTarget.patientRole.patient.name.given AS first_name,
    recordTarget.patientRole.patient.name.family AS last_name,
    entry_array.substanceAdministration.consumable.manufacturedProduct
        .manufacturedMaterial.code._displayName AS medication_name
FROM (
    SELECT *, explode(component_array.section.entry) AS entry_array
    FROM (
        SELECT *, explode(component.structuredBody.component) AS component_array
        FROM {raw_table}
    )
    WHERE component_array.section.title = 'Medications'
)
""")

print(f"Medications table created: {med_table}")

# COMMAND ----------

# DBTITLE 1,Preview medications
display(spark.sql(f"SELECT * FROM {med_table} LIMIT 20"))

# COMMAND ----------

# DBTITLE 1,Medication frequency
display(spark.sql(f"""
    SELECT medication_name, count(1) AS patient_count
    FROM {med_table}
    GROUP BY medication_name
    ORDER BY patient_count DESC
    LIMIT 20
"""))
