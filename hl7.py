# Databricks notebook source
# COMPUTE: Serverless

# MAGIC %md
# MAGIC # HL7v2 ADT Message Parsing
# MAGIC
# MAGIC Demonstrates parsing HL7v2 ADT messages using Python. The original workshop used
# MAGIC a Scala notebook with the Smolder library — this version uses `python-hl7` for
# MAGIC portability and Serverless compatibility.
# MAGIC
# MAGIC HL7v2 is the most common message format for real-time hospital events (admissions,
# MAGIC transfers, discharges). This notebook reads HL7v2 files from a UC Volume, parses
# MAGIC the segment structure, and extracts patient and diagnosis information into a Delta table.
# MAGIC
# MAGIC **Synthetic data only — no PHI.**

# COMMAND ----------

# MAGIC %pip install python-hl7 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "hls_workshop", "Target Catalog")
dbutils.widgets.text("schema", "hl7", "Target Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ql = f"`{catalog}`.`{schema}`"

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ql}")

# Volume for HL7 message files
spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {ql}.`raw_files`
COMMENT 'Landing zone for HL7v2 message files'
""")
volume_path = f"/Volumes/{catalog}/{schema}/raw_files"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Synthetic HL7v2 Messages
# MAGIC
# MAGIC In production, HL7v2 messages arrive via a Mirth/Rhapsody interface engine and
# MAGIC land as files in the Volume. For this workshop, we generate synthetic ADT_A01
# MAGIC (admission) messages.

# COMMAND ----------

# DBTITLE 1,Generate sample HL7 ADT messages
import random
import datetime

def generate_hl7_adt(patient_id: str, admit_date: str, diagnosis_code: str, facility: str) -> str:
    """Generate a minimal HL7v2 ADT_A01 message for workshop purposes."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    msg_id = f"MSG{random.randint(100000, 999999)}"
    return (
        f"MSH|^~\\&|{facility}|{facility}|RECEIVING|RECEIVING|{timestamp}||ADT^A01|{msg_id}|P|2.5\r"
        f"EVN|A01|{timestamp}||||||{facility}\r"
        f"PID|||{patient_id}^^^{facility}||DOE^JOHN||19800101|M|||123 MAIN ST^^LINCOLN^NE^68501\r"
        f"PV1||I|{facility}^^^||||1234^SMITH^JANE|||MED||||||||1234^SMITH^JANE|IP||||||||||||||||||||||||||{admit_date}\r"
        f"DG1|1|ICD10|{diagnosis_code}||{admit_date}|A\r"
    )

# Sample data: mix of diagnoses across facilities
sample_messages = []
facilities = ["MEMORIAL_COMMUNITY", "FRANKLIN_COUNTY", "PHELPS_MEMORIAL", "KEARNEY_REGIONAL"]
diagnoses = [
    ("I10", "Essential hypertension"),
    ("E11.9", "Type 2 diabetes"),
    ("J44.1", "COPD with acute exacerbation"),
    ("I50.9", "Heart failure, unspecified"),
    ("N18.3", "Chronic kidney disease, stage 3"),
    ("S72.001A", "Hip fracture"),
    ("J18.9", "Pneumonia"),
    ("I21.9", "Acute MI"),
]

for i in range(50):
    pid = f"PAT{1000 + i}"
    admit = f"2026{random.randint(1,12):02d}{random.randint(1,28):02d}"
    dx_code, _ = random.choice(diagnoses)
    fac = random.choice(facilities)
    sample_messages.append(generate_hl7_adt(pid, admit, dx_code, fac))

# Write to Volume as individual files
hl7_dir = f"{volume_path}/hl7_messages"
dbutils.fs.mkdirs(hl7_dir)
for idx, msg in enumerate(sample_messages):
    dbutils.fs.put(f"{hl7_dir}/adt_{idx:04d}.hl7", msg, overwrite=True)

print(f"Generated {len(sample_messages)} HL7v2 ADT messages in {hl7_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parse HL7v2 Messages
# MAGIC
# MAGIC Read the HL7 files as raw text, then use a UDF to parse the segment structure.
# MAGIC The `python-hl7` library handles the HL7v2 segment/field/component delimiters.

# COMMAND ----------

# DBTITLE 1,HL7 parsing UDF
import hl7
from pyspark.sql.functions import col, udf, current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType


def parse_hl7_message(raw_text: str) -> dict:
    """Parse an HL7v2 message and extract ADT-relevant fields.

    Extracts:
    - patient_id from PID-3
    - admit_location from EVN-7 (or PV1-3)
    - admit_diagnosis from DG1-3
    - message_type from MSH-9
    - facility from MSH-3
    """
    try:
        # python-hl7 expects \r as segment separator
        cleaned = raw_text.replace("\n", "\r")
        msg = hl7.parse(cleaned)

        # MSH-9: message type (e.g., ADT^A01)
        msg_type = str(msg.segment("MSH")[9]) if msg.segment("MSH") else ""

        # MSH-3: sending facility
        facility = str(msg.segment("MSH")[3]) if msg.segment("MSH") else ""

        # PID-3: patient ID
        patient_id = ""
        try:
            patient_id = str(msg.segment("PID")[3])
        except (KeyError, IndexError):
            pass

        # EVN-7: event facility (fallback to MSH-3)
        admit_location = ""
        try:
            admit_location = str(msg.segment("EVN")[7]) or facility
        except (KeyError, IndexError):
            admit_location = facility

        # DG1-3: diagnosis code
        admit_diagnosis = ""
        try:
            admit_diagnosis = str(msg.segment("DG1")[3])
        except (KeyError, IndexError):
            pass

        return {
            "patient_id": patient_id,
            "admit_location": admit_location,
            "admit_diagnosis": admit_diagnosis,
            "message_type": msg_type,
            "facility": facility,
        }
    except Exception as e:
        return {
            "patient_id": "",
            "admit_location": "",
            "admit_diagnosis": "",
            "message_type": "",
            "facility": f"PARSE_ERROR: {str(e)[:100]}",
        }


parse_schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("admit_location", StringType(), True),
    StructField("admit_diagnosis", StringType(), True),
    StructField("message_type", StringType(), True),
    StructField("facility", StringType(), True),
])

parse_hl7_udf = udf(parse_hl7_message, parse_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Batch Process HL7 Files

# COMMAND ----------

# DBTITLE 1,Read and parse HL7 messages
raw_df = spark.read.text(f"{hl7_dir}/*.hl7", wholetext=True)

parsed_df = (
    raw_df
    .withColumn("parsed", parse_hl7_udf(col("value")))
    .select(
        col("parsed.patient_id"),
        col("parsed.admit_location"),
        col("parsed.admit_diagnosis"),
        col("parsed.message_type"),
        col("parsed.facility"),
        input_file_name().alias("source_file"),
        current_timestamp().alias("parsed_at"),
    )
)

# Write to Delta
target_table = f"{ql}.`parsed_hl7`"
parsed_df.write.format("delta").mode("overwrite").saveAsTable(target_table)

print(f"Parsed {parsed_df.count()} HL7 messages to {target_table}")
display(parsed_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Admission Diagnosis Summary

# COMMAND ----------

# DBTITLE 1,Top diagnoses by frequency
display(spark.sql(f"""
    SELECT admit_diagnosis, facility, count(1) AS admission_count
    FROM {target_table}
    WHERE admit_diagnosis IS NOT NULL AND admit_diagnosis != ''
    GROUP BY admit_diagnosis, facility
    ORDER BY admission_count DESC
    LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Admissions by facility
display(spark.sql(f"""
    SELECT facility, count(1) AS admission_count
    FROM {target_table}
    WHERE message_type LIKE '%A01%'
    GROUP BY facility
    ORDER BY admission_count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Streaming (Production Pattern)
# MAGIC
# MAGIC In production, HL7 messages arrive continuously via an interface engine
# MAGIC (Mirth Connect, Rhapsody) writing to the Volume. Auto Loader or Structured
# MAGIC Streaming picks up new files incrementally.
# MAGIC
# MAGIC ```python
# MAGIC # Production streaming pattern (requires Serverless or dedicated cluster)
# MAGIC stream_df = (
# MAGIC     spark.readStream
# MAGIC     .format("text")
# MAGIC     .option("wholetext", "true")
# MAGIC     .option("maxFilesPerTrigger", 10)
# MAGIC     .load(f"{hl7_dir}/*.hl7")
# MAGIC )
# MAGIC
# MAGIC parsed_stream = stream_df.withColumn("parsed", parse_hl7_udf(col("value")))
# MAGIC
# MAGIC (parsed_stream
# MAGIC     .select("parsed.*", input_file_name().alias("source_file"))
# MAGIC     .writeStream
# MAGIC     .format("delta")
# MAGIC     .outputMode("append")
# MAGIC     .option("checkpointLocation", f"{volume_path}/checkpoints/hl7_stream")
# MAGIC     .toTable(target_table)
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC The streaming pattern is shown here as documentation. For the workshop,
# MAGIC batch processing demonstrates the same parsing logic without requiring
# MAGIC a long-running streaming cluster.
