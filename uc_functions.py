# Databricks notebook source
# COMPUTE: Serverless

# MAGIC %md
# MAGIC # Unity Catalog Functions for Healthcare Interoperability
# MAGIC
# MAGIC Registers reusable SQL and Python UDFs in Unity Catalog. These functions
# MAGIC are governed, discoverable, and shareable across notebooks, dashboards,
# MAGIC Genie spaces, and external tools (Tableau, dbt).
# MAGIC
# MAGIC UC functions are the mechanism that turns one-off notebook logic into
# MAGIC auditable, reusable analytics infrastructure.
# MAGIC
# MAGIC **Synthetic data only — no PHI.**

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "hls_workshop", "Workshop Catalog")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`functions`")

fn = f"`{catalog}`.`functions`"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clinical Code Lookup Functions
# MAGIC
# MAGIC Translate SNOMED, ICD-10, and HL7 codes to human-readable descriptions.
# MAGIC These are the functions a care coordinator or analyst calls when a dashboard
# MAGIC shows a code they don't recognize.

# COMMAND ----------

# DBTITLE 1,fn: snomed_description — Translate SNOMED CT codes
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fn}.`snomed_description`(code STRING)
RETURNS STRING
COMMENT 'Returns a human-readable description for common SNOMED CT codes used in FHIR resources. Returns the code itself if not in the lookup.'
RETURN CASE code
    WHEN '44054006' THEN 'Diabetes mellitus type 2'
    WHEN '73211009' THEN 'Diabetes mellitus type 1'
    WHEN '46635009' THEN 'Diabetes mellitus type 1.5'
    WHEN '59621000' THEN 'Essential hypertension'
    WHEN '88805009' THEN 'Chronic congestive heart failure'
    WHEN '13645005' THEN 'Chronic obstructive pulmonary disease'
    WHEN '709044004' THEN 'Chronic kidney disease'
    WHEN '427089005' THEN 'Diabetes mellitus due to genetic defect'
    WHEN '127013003' THEN 'Diabetic renal disease'
    WHEN '422034002' THEN 'Diabetic retinopathy'
    WHEN '55680006' THEN 'Drug overdose'
    WHEN '361055000' THEN 'Misuses drugs'
    WHEN '449868002' THEN 'Current every day smoker'
    WHEN '162673000' THEN 'General examination of patient'
    WHEN '185349003' THEN 'Encounter for check up'
    WHEN '410620009' THEN 'Well child visit'
    WHEN '50849002' THEN 'Emergency room admission'
    WHEN '32485007' THEN 'Hospital admission'
    WHEN '24623002' THEN 'Screening mammography'
    WHEN '73761001' THEN 'Colonoscopy'
    ELSE code
END
""")

print(f"Created: {fn}.snomed_description")

# Test
display(spark.sql(f"SELECT {fn}.`snomed_description`('44054006') AS description"))

# COMMAND ----------

# DBTITLE 1,fn: icd10_description — Translate ICD-10-CM codes
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fn}.`icd10_description`(code STRING)
RETURNS STRING
COMMENT 'Returns a human-readable description for common ICD-10-CM codes seen in HL7 ADT messages. Returns the code itself if not in the lookup.'
RETURN CASE code
    WHEN 'I10'      THEN 'Essential hypertension'
    WHEN 'E11.9'    THEN 'Type 2 diabetes mellitus without complications'
    WHEN 'J44.1'    THEN 'COPD with acute exacerbation'
    WHEN 'I50.9'    THEN 'Heart failure, unspecified'
    WHEN 'N18.3'    THEN 'Chronic kidney disease, stage 3'
    WHEN 'S72.001A' THEN 'Fracture of unspecified part of neck of right femur'
    WHEN 'J18.9'    THEN 'Pneumonia, unspecified organism'
    WHEN 'I21.9'    THEN 'Acute myocardial infarction, unspecified'
    WHEN 'I25.10'   THEN 'Atherosclerotic heart disease of native coronary artery'
    WHEN 'J96.01'   THEN 'Acute respiratory failure with hypoxia'
    WHEN 'K92.1'    THEN 'Melena (GI bleed)'
    ELSE code
END
""")

print(f"Created: {fn}.icd10_description")

# COMMAND ----------

# DBTITLE 1,fn: adt_event_description — Translate HL7 ADT event types
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fn}.`adt_event_description`(event_code STRING)
RETURNS STRING
COMMENT 'Returns the clinical meaning of an HL7 ADT event code (e.g., A01 = patient admitted).'
RETURN CASE event_code
    WHEN 'A01' THEN 'Patient admitted'
    WHEN 'A02' THEN 'Patient transferred'
    WHEN 'A03' THEN 'Patient discharged'
    WHEN 'A04' THEN 'Patient registered'
    WHEN 'A05' THEN 'Patient pre-admitted'
    WHEN 'A06' THEN 'Outpatient to inpatient transfer'
    WHEN 'A07' THEN 'Inpatient to outpatient transfer'
    WHEN 'A08' THEN 'Patient information updated'
    WHEN 'A11' THEN 'Admission cancelled'
    WHEN 'A13' THEN 'Discharge cancelled'
    WHEN 'A28' THEN 'Person added'
    WHEN 'A31' THEN 'Person information updated'
    ELSE concat('Unknown ADT event: ', event_code)
END
""")

print(f"Created: {fn}.adt_event_description")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clinical Computation Functions
# MAGIC
# MAGIC Reusable calculations that enforce consistent business logic across
# MAGIC dashboards, Genie queries, and ad-hoc analysis.

# COMMAND ----------

# DBTITLE 1,fn: age_from_dob — Compute age from date of birth
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fn}.`age_from_dob`(birth_date DATE)
RETURNS INT
COMMENT 'Computes age in years from a date of birth. Handles leap years correctly.'
RETURN floor(datediff(current_date(), birth_date) / 365.25)
""")

print(f"Created: {fn}.age_from_dob")

# COMMAND ----------

# DBTITLE 1,fn: chronic_risk_tier — Classify comorbidity burden
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fn}.`chronic_risk_tier`(condition_count INT)
RETURNS STRING
COMMENT 'Classifies a patient into a risk tier based on number of tracked chronic conditions (HTN, DM, CHF, COPD, CKD). Used for outreach prioritization.'
RETURN CASE
    WHEN condition_count >= 3 THEN 'high'
    WHEN condition_count = 2 THEN 'moderate'
    WHEN condition_count = 1 THEN 'low'
    ELSE 'none'
END
""")

print(f"Created: {fn}.chronic_risk_tier")

# COMMAND ----------

# DBTITLE 1,fn: days_overdue_bucket — Categorize engagement status
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fn}.`days_overdue_bucket`(days_since_encounter INT)
RETURNS STRING
COMMENT 'Categorizes patients by days since last clinical encounter. Used by care coordinators to identify patients lost to care.'
RETURN CASE
    WHEN days_since_encounter IS NULL THEN 'unknown'
    WHEN days_since_encounter <= 90 THEN '0-90 days (active)'
    WHEN days_since_encounter <= 180 THEN '91-180 days (at risk)'
    WHEN days_since_encounter <= 365 THEN '181-365 days (overdue)'
    ELSE '365+ days (lost to care)'
END
""")

print(f"Created: {fn}.days_overdue_bucket")

# COMMAND ----------

# DBTITLE 1,fn: hedis_measure_name — Expand HEDIS measure codes
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fn}.`hedis_measure_name`(measure_code STRING)
RETURNS STRING
COMMENT 'Returns the full HEDIS measure name from a short code. Used in dashboard titles and Genie responses.'
RETURN CASE upper(measure_code)
    WHEN 'AWV' THEN 'Annual Wellness Visit'
    WHEN 'BCS' THEN 'Breast Cancer Screening'
    WHEN 'COL' THEN 'Colorectal Cancer Screening'
    WHEN 'A1C' THEN 'HbA1c Management (<8%)'
    WHEN 'CBP' THEN 'Controlling High Blood Pressure'
    WHEN 'PCR' THEN 'Plan All-Cause Readmissions'
    ELSE concat('Unknown measure: ', measure_code)
END
""")

print(f"Created: {fn}.hedis_measure_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Quality Functions

# COMMAND ----------

# DBTITLE 1,fn: is_valid_snomed — Validate SNOMED code format
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fn}.`is_valid_snomed`(code STRING)
RETURNS BOOLEAN
COMMENT 'Returns true if the input looks like a valid SNOMED CT code (6-18 digit numeric). Does not validate against a terminology server.'
RETURN code RLIKE '^[0-9]{{6,18}}$'
""")

print(f"Created: {fn}.is_valid_snomed")

# COMMAND ----------

# DBTITLE 1,fn: is_valid_icd10 — Validate ICD-10-CM code format
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fn}.`is_valid_icd10`(code STRING)
RETURNS BOOLEAN
COMMENT 'Returns true if the input matches ICD-10-CM format (letter + 2 digits, optional dot + up to 4 chars). Does not validate against a code set.'
RETURN code RLIKE '^[A-Z][0-9]{{2}}(\\.[A-Z0-9]{{1,4}})?$'
""")

print(f"Created: {fn}.is_valid_icd10")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Function Summary
# MAGIC
# MAGIC All functions are registered in `{catalog}.functions` and are available to:
# MAGIC - SQL queries in notebooks and dashboards
# MAGIC - Genie spaces (Genie can call UC functions in generated SQL)
# MAGIC - Tableau custom SQL
# MAGIC - dbt models
# MAGIC - Any JDBC/ODBC client

# COMMAND ----------

# DBTITLE 1,List all registered functions
display(spark.sql(f"SHOW FUNCTIONS IN {fn}"))

# COMMAND ----------

# DBTITLE 1,Example: Enrich HL7 data with UC functions
display(spark.sql(f"""
    SELECT
        patient_id,
        facility,
        admit_diagnosis,
        {fn}.`icd10_description`(admit_diagnosis) AS diagnosis_description,
        message_type,
        {fn}.`adt_event_description`(
            regexp_extract(message_type, 'A[0-9]+', 0)
        ) AS event_description
    FROM `{catalog}`.`hl7`.`parsed_hl7`
    LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1,Example: Enrich OMOP conditions with UC functions
display(spark.sql(f"""
    SELECT
        person_id,
        condition_source_value,
        {fn}.`snomed_description`(condition_source_value) AS condition_name,
        {fn}.`is_valid_snomed`(condition_source_value) AS valid_snomed
    FROM `{catalog}`.`fhir`.`condition_occurrence`
    LIMIT 10
"""))
