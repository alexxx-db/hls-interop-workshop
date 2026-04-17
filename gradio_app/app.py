"""
Healthcare Interoperability Workshop — Databricks App (Gradio)

A unified interface for exploring the four interoperability patterns
demonstrated in the workshop: JDBC, CDA/CCDA XML, FHIR/OMOP, and HL7v2.

Runs as a Databricks App on Serverless compute. Connects to the workshop
catalog via the Databricks SQL Connector.

Synthetic data only — no PHI.
"""

import os
from typing import Optional

import gradio as gr
import pandas as pd
from databricks import sql as dbsql

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATALOG = os.environ.get("CATALOG", "hls_workshop")
WAREHOUSE_HTTP_PATH = os.environ.get(
    "DATABRICKS_SQL_WAREHOUSE_HTTP_PATH",
    "/sql/1.0/warehouses/auto",
)
HOST = os.environ.get("DATABRICKS_HOST", "")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

_connection = None


def get_connection():
    """Create or reuse a Databricks SQL connection."""
    global _connection
    if _connection is None:
        _connection = dbsql.connect(
            server_hostname=HOST,
            http_path=WAREHOUSE_HTTP_PATH,
            access_token=TOKEN,
        )
    return _connection


def run_query(query: str) -> pd.DataFrame:
    """Execute a SQL query and return a pandas DataFrame."""
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)


def safe_query(query: str) -> pd.DataFrame:
    """Run a query, returning an empty DataFrame on error."""
    try:
        return run_query(query)
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


def scalar_query(query: str, default: str = "0") -> str:
    """Return a single scalar value as a string."""
    df = safe_query(query)
    if df.empty or "error" in df.columns:
        return default
    return str(df.iloc[0, 0])


# ---------------------------------------------------------------------------
# Tab: Overview
# ---------------------------------------------------------------------------

def overview_kpis():
    """Return KPI values for all four workshop patterns."""
    ccda_docs = scalar_query(f"SELECT count(*) FROM `{CATALOG}`.`ccda`.`ccda_claims_raw`")
    meds = scalar_query(f"SELECT count(*) FROM `{CATALOG}`.`ccda`.`medications`")
    omop = scalar_query(f"SELECT count(*) FROM `{CATALOG}`.`fhir`.`person`")
    hl7 = scalar_query(f"SELECT count(*) FROM `{CATALOG}`.`hl7`.`parsed_hl7`")
    return ccda_docs, meds, omop, hl7


def overview_inventory():
    """Return the table inventory across all schemas."""
    return safe_query(f"""
        SELECT 'ccda' AS schema_name, 'ccda_claims_raw' AS table_name, count(*) AS rows
        FROM `{CATALOG}`.`ccda`.`ccda_claims_raw`
        UNION ALL SELECT 'ccda', 'medications', count(*) FROM `{CATALOG}`.`ccda`.`medications`
        UNION ALL SELECT 'fhir', 'person', count(*) FROM `{CATALOG}`.`fhir`.`person`
        UNION ALL SELECT 'fhir', 'condition_occurrence', count(*) FROM `{CATALOG}`.`fhir`.`condition_occurrence`
        UNION ALL SELECT 'fhir', 'procedure_occurrence', count(*) FROM `{CATALOG}`.`fhir`.`procedure_occurrence`
        UNION ALL SELECT 'fhir', 'visit_occurrence', count(*) FROM `{CATALOG}`.`fhir`.`visit_occurrence`
        UNION ALL SELECT 'fhir', 'condition_features', count(*) FROM `{CATALOG}`.`fhir`.`condition_features`
        UNION ALL SELECT 'hl7', 'parsed_hl7', count(*) FROM `{CATALOG}`.`hl7`.`parsed_hl7`
        ORDER BY schema_name, table_name
    """)


def build_overview_tab():
    """Build the Overview tab."""
    with gr.Column():
        gr.Markdown("## Workshop Data Summary")
        gr.Markdown(
            "Four healthcare data integration patterns: "
            "**JDBC**, **CDA/CCDA XML**, **FHIR/OMOP**, and **HL7v2 ADT**."
        )

        with gr.Row():
            ccda_box = gr.Textbox(label="CCDA Documents", interactive=False)
            meds_box = gr.Textbox(label="Medications Extracted", interactive=False)
            omop_box = gr.Textbox(label="OMOP Patients", interactive=False)
            hl7_box = gr.Textbox(label="HL7 Messages", interactive=False)

        refresh_btn = gr.Button("Refresh KPIs", variant="primary")
        refresh_btn.click(overview_kpis, outputs=[ccda_box, meds_box, omop_box, hl7_box])

        gr.Markdown("### Table Inventory")
        inventory_table = gr.DataFrame(label="Tables", interactive=False)
        inv_btn = gr.Button("Load Inventory")
        inv_btn.click(overview_inventory, outputs=inventory_table)


# ---------------------------------------------------------------------------
# Tab: CCDA Medications
# ---------------------------------------------------------------------------

def ccda_top_medications(limit: int = 15):
    """Top medications by patient count."""
    return safe_query(f"""
        SELECT medication_name, count(*) AS patients
        FROM `{CATALOG}`.`ccda`.`medications`
        WHERE medication_name IS NOT NULL
        GROUP BY medication_name
        ORDER BY patients DESC
        LIMIT {int(limit)}
    """)


def ccda_by_city():
    """Prescriptions by city."""
    return safe_query(f"""
        SELECT city, count(*) AS prescriptions
        FROM `{CATALOG}`.`ccda`.`medications`
        WHERE city IS NOT NULL
        GROUP BY city
        ORDER BY prescriptions DESC
        LIMIT 10
    """)


def ccda_records(limit: int = 50):
    """Browsable medication records."""
    return safe_query(f"""
        SELECT city, first_name, last_name, medication_name
        FROM `{CATALOG}`.`ccda`.`medications`
        LIMIT {int(limit)}
    """)


def build_ccda_tab():
    """Build the CCDA Medications tab."""
    with gr.Column():
        gr.Markdown("## CDA/CCDA — Medication Analysis")
        gr.Markdown("Medications extracted from Synthea CCDA XML clinical documents.")

        with gr.Row():
            with gr.Column():
                gr.Markdown("### Top Medications")
                med_limit = gr.Slider(5, 30, value=15, step=1, label="Top N")
                med_table = gr.DataFrame(label="Medications", interactive=False)
                med_btn = gr.Button("Load Medications")
                med_btn.click(ccda_top_medications, inputs=med_limit, outputs=med_table)

            with gr.Column():
                gr.Markdown("### Prescriptions by City")
                city_table = gr.DataFrame(label="Cities", interactive=False)
                city_btn = gr.Button("Load Cities")
                city_btn.click(ccda_by_city, outputs=city_table)

        gr.Markdown("### Medication Records")
        rec_limit = gr.Slider(10, 200, value=50, step=10, label="Rows to display")
        rec_table = gr.DataFrame(label="Records", interactive=False)
        rec_btn = gr.Button("Load Records")
        rec_btn.click(ccda_records, inputs=rec_limit, outputs=rec_table)


# ---------------------------------------------------------------------------
# Tab: FHIR / OMOP
# ---------------------------------------------------------------------------

def fhir_prevalence():
    """Chronic condition prevalence."""
    return safe_query(f"""
        SELECT 'Diabetes' AS condition, sum(diabetes_ind) AS patients
        FROM `{CATALOG}`.`fhir`.`condition_features`
        UNION ALL SELECT 'Hypertension', sum(hypertension_ind) FROM `{CATALOG}`.`fhir`.`condition_features`
        UNION ALL SELECT 'Substance Use', sum(substance_use_ind) FROM `{CATALOG}`.`fhir`.`condition_features`
    """)


def fhir_omop_sizes():
    """OMOP table row counts."""
    return safe_query(f"""
        SELECT 'person' AS table_name, count(*) AS rows FROM `{CATALOG}`.`fhir`.`person`
        UNION ALL SELECT 'condition_occurrence', count(*) FROM `{CATALOG}`.`fhir`.`condition_occurrence`
        UNION ALL SELECT 'procedure_occurrence', count(*) FROM `{CATALOG}`.`fhir`.`procedure_occurrence`
        UNION ALL SELECT 'visit_occurrence', count(*) FROM `{CATALOG}`.`fhir`.`visit_occurrence`
    """)


def fhir_conditions_enriched(limit: int = 50):
    """Conditions enriched with UC function."""
    return safe_query(f"""
        SELECT
            person_id,
            condition_source_value AS code,
            `{CATALOG}`.`functions`.`snomed_description`(condition_source_value) AS description,
            `{CATALOG}`.`functions`.`is_valid_snomed`(condition_source_value) AS valid_snomed
        FROM `{CATALOG}`.`fhir`.`condition_occurrence`
        LIMIT {int(limit)}
    """)


def build_fhir_tab():
    """Build the FHIR/OMOP tab."""
    with gr.Column():
        gr.Markdown("## FHIR-to-OMOP — Clinical Profile")

        with gr.Row():
            with gr.Column():
                gr.Markdown("### Chronic Condition Prevalence")
                prev_table = gr.DataFrame(label="Prevalence", interactive=False)
                prev_btn = gr.Button("Load Prevalence")
                prev_btn.click(fhir_prevalence, outputs=prev_table)

            with gr.Column():
                gr.Markdown("### OMOP Table Sizes")
                size_table = gr.DataFrame(label="Sizes", interactive=False)
                size_btn = gr.Button("Load Sizes")
                size_btn.click(fhir_omop_sizes, outputs=size_table)

        gr.Markdown("### Condition Records (enriched with `snomed_description()` UC function)")
        cond_limit = gr.Slider(10, 200, value=50, step=10, label="Rows")
        cond_table = gr.DataFrame(label="Conditions", interactive=False)
        cond_btn = gr.Button("Load Conditions")
        cond_btn.click(fhir_conditions_enriched, inputs=cond_limit, outputs=cond_table)


# ---------------------------------------------------------------------------
# Tab: HL7 Admissions
# ---------------------------------------------------------------------------

def hl7_facilities():
    """List of facilities for dropdown."""
    df = safe_query(f"""
        SELECT DISTINCT facility FROM `{CATALOG}`.`hl7`.`parsed_hl7`
        WHERE facility IS NOT NULL ORDER BY facility
    """)
    if "error" in df.columns or df.empty:
        return ["All"]
    return ["All"] + df["facility"].tolist()


def hl7_by_facility(facility: str):
    """Admissions by facility."""
    where = f"WHERE facility = '{facility}'" if facility != "All" else ""
    return safe_query(f"""
        SELECT facility, count(*) AS admissions
        FROM `{CATALOG}`.`hl7`.`parsed_hl7`
        {where}
        GROUP BY facility ORDER BY admissions DESC
    """)


def hl7_top_diagnoses(facility: str):
    """Top diagnoses with ICD-10 descriptions."""
    where = f"AND facility = '{facility}'" if facility != "All" else ""
    return safe_query(f"""
        SELECT
            admit_diagnosis AS code,
            `{CATALOG}`.`functions`.`icd10_description`(admit_diagnosis) AS diagnosis,
            count(*) AS frequency
        FROM `{CATALOG}`.`hl7`.`parsed_hl7`
        WHERE admit_diagnosis IS NOT NULL AND admit_diagnosis != '' {where}
        GROUP BY admit_diagnosis ORDER BY frequency DESC
        LIMIT 10
    """)


def hl7_messages(facility: str, limit: int = 20):
    """HL7 message detail table."""
    where = f"WHERE facility = '{facility}'" if facility != "All" else ""
    return safe_query(f"""
        SELECT
            patient_id, facility, message_type,
            admit_diagnosis,
            `{CATALOG}`.`functions`.`icd10_description`(admit_diagnosis) AS diagnosis_name,
            `{CATALOG}`.`functions`.`adt_event_description`(
                regexp_extract(message_type, 'A[0-9]+', 0)
            ) AS event_description,
            parsed_at
        FROM `{CATALOG}`.`hl7`.`parsed_hl7`
        {where}
        ORDER BY parsed_at DESC
        LIMIT {int(limit)}
    """)


def build_hl7_tab():
    """Build the HL7 Admissions tab."""
    with gr.Column():
        gr.Markdown("## HL7v2 — ADT Admission Analysis")

        facility_dd = gr.Dropdown(
            choices=hl7_facilities(),
            value="All",
            label="Filter by Facility",
        )

        with gr.Row():
            with gr.Column():
                gr.Markdown("### Admissions by Facility")
                fac_table = gr.DataFrame(label="Facilities", interactive=False)
                fac_btn = gr.Button("Load Facility Data")
                fac_btn.click(hl7_by_facility, inputs=facility_dd, outputs=fac_table)

            with gr.Column():
                gr.Markdown("### Top Admit Diagnoses")
                dx_table = gr.DataFrame(label="Diagnoses", interactive=False)
                dx_btn = gr.Button("Load Diagnoses")
                dx_btn.click(hl7_top_diagnoses, inputs=facility_dd, outputs=dx_table)

        gr.Markdown("### Message Detail")
        msg_limit = gr.Slider(10, 100, value=20, step=10, label="Rows")
        msg_table = gr.DataFrame(label="Messages", interactive=False)
        msg_btn = gr.Button("Load Messages")
        msg_btn.click(hl7_messages, inputs=[facility_dd, msg_limit], outputs=msg_table)


# ---------------------------------------------------------------------------
# Tab: UC Functions Explorer
# ---------------------------------------------------------------------------

def lookup_snomed(code: str) -> str:
    """Look up a SNOMED code."""
    if not code.strip():
        return ""
    return scalar_query(
        f"SELECT `{CATALOG}`.`functions`.`snomed_description`('{code}')", default=code
    )


def lookup_icd10(code: str) -> str:
    """Look up an ICD-10-CM code."""
    if not code.strip():
        return ""
    return scalar_query(
        f"SELECT `{CATALOG}`.`functions`.`icd10_description`('{code}')", default=code
    )


def lookup_adt(code: str) -> str:
    """Look up an ADT event code."""
    if not code.strip():
        return ""
    return scalar_query(
        f"SELECT `{CATALOG}`.`functions`.`adt_event_description`('{code}')", default=code
    )


def classify_risk(count: int) -> str:
    """Classify risk tier by condition count."""
    return scalar_query(
        f"SELECT `{CATALOG}`.`functions`.`chronic_risk_tier`({int(count)})", default="none"
    )


def validate_code(code: str) -> str:
    """Validate a clinical code."""
    if not code.strip():
        return ""
    df = safe_query(f"""
        SELECT
            `{CATALOG}`.`functions`.`is_valid_snomed`('{code}') AS valid_snomed,
            `{CATALOG}`.`functions`.`is_valid_icd10`('{code}') AS valid_icd10
    """)
    if df.empty or "error" in df.columns:
        return "Unable to validate"
    row = df.iloc[0]
    parts = []
    if row["valid_snomed"]:
        parts.append("Valid SNOMED")
    if row["valid_icd10"]:
        parts.append("Valid ICD-10-CM")
    return ", ".join(parts) if parts else "Not a recognized SNOMED or ICD-10 format"


def list_functions():
    """List all registered UC functions."""
    return safe_query(f"SHOW FUNCTIONS IN `{CATALOG}`.`functions`")


def build_uc_functions_tab():
    """Build the UC Functions Explorer tab."""
    with gr.Column():
        gr.Markdown("## Unity Catalog Functions")
        gr.Markdown(
            "Reusable SQL functions registered in Unity Catalog. Available to "
            "notebooks, dashboards, Genie, Tableau, and any JDBC client."
        )

        with gr.Row():
            with gr.Column():
                gr.Markdown("### SNOMED Lookup")
                snomed_in = gr.Textbox(label="SNOMED code", value="44054006")
                snomed_out = gr.Textbox(label="Description", interactive=False)
                snomed_btn = gr.Button("Look up")
                snomed_btn.click(lookup_snomed, inputs=snomed_in, outputs=snomed_out)

            with gr.Column():
                gr.Markdown("### ICD-10 Lookup")
                icd_in = gr.Textbox(label="ICD-10-CM code", value="I10")
                icd_out = gr.Textbox(label="Description", interactive=False)
                icd_btn = gr.Button("Look up")
                icd_btn.click(lookup_icd10, inputs=icd_in, outputs=icd_out)

            with gr.Column():
                gr.Markdown("### ADT Event Lookup")
                adt_in = gr.Textbox(label="ADT event code", value="A01")
                adt_out = gr.Textbox(label="Description", interactive=False)
                adt_btn = gr.Button("Look up")
                adt_btn.click(lookup_adt, inputs=adt_in, outputs=adt_out)

        with gr.Row():
            with gr.Column():
                gr.Markdown("### Risk Tier")
                risk_in = gr.Slider(0, 5, value=2, step=1, label="Chronic condition count")
                risk_out = gr.Textbox(label="Risk tier", interactive=False)
                risk_btn = gr.Button("Classify")
                risk_btn.click(classify_risk, inputs=risk_in, outputs=risk_out)

            with gr.Column():
                gr.Markdown("### Code Validation")
                val_in = gr.Textbox(label="Code to validate", value="44054006")
                val_out = gr.Textbox(label="Validation result", interactive=False)
                val_btn = gr.Button("Validate")
                val_btn.click(validate_code, inputs=val_in, outputs=val_out)

        gr.Markdown("### Registered Functions")
        fn_table = gr.DataFrame(label="Functions", interactive=False)
        fn_btn = gr.Button("List Functions")
        fn_btn.click(list_functions, outputs=fn_table)


# ---------------------------------------------------------------------------
# Tab: Data Quality
# ---------------------------------------------------------------------------

def dq_hl7():
    """HL7 parse quality metrics."""
    return safe_query(f"""
        SELECT
            count(*) AS total_messages,
            sum(CASE WHEN patient_id IS NULL OR patient_id = '' THEN 1 ELSE 0 END) AS missing_patient_id,
            sum(CASE WHEN admit_diagnosis IS NULL OR admit_diagnosis = '' THEN 1 ELSE 0 END) AS missing_diagnosis,
            sum(CASE WHEN facility IS NULL OR facility = '' THEN 1 ELSE 0 END) AS missing_facility,
            round(
                sum(CASE WHEN patient_id IS NOT NULL AND patient_id != '' THEN 1 ELSE 0 END) * 100.0 / count(*), 1
            ) AS patient_id_pct
        FROM `{CATALOG}`.`hl7`.`parsed_hl7`
    """)


def dq_snomed():
    """SNOMED code validity check."""
    return safe_query(f"""
        SELECT
            count(*) AS total,
            sum(CASE WHEN `{CATALOG}`.`functions`.`is_valid_snomed`(condition_source_value) THEN 1 ELSE 0 END) AS valid,
            round(
                sum(CASE WHEN `{CATALOG}`.`functions`.`is_valid_snomed`(condition_source_value) THEN 1 ELSE 0 END) * 100.0 / count(*), 1
            ) AS valid_pct
        FROM `{CATALOG}`.`fhir`.`condition_occurrence`
    """)


def dq_ccda():
    """CCDA medication completeness."""
    return safe_query(f"""
        SELECT
            count(*) AS total,
            sum(CASE WHEN medication_name IS NULL THEN 1 ELSE 0 END) AS null_medication,
            sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS null_city,
            sum(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_name
        FROM `{CATALOG}`.`ccda`.`medications`
    """)


def dq_audit():
    """JDBC audit log."""
    return safe_query(f"""
        SELECT * FROM `{CATALOG}`.`audit`.`jdbc_ingests`
        ORDER BY refresh_date DESC LIMIT 20
    """)


def build_dq_tab():
    """Build the Data Quality tab."""
    with gr.Column():
        gr.Markdown("## Data Quality Overview")

        gr.Markdown("### HL7 Parse Quality")
        hl7_dq = gr.DataFrame(label="HL7 Quality", interactive=False)
        hl7_btn = gr.Button("Check HL7 Quality")
        hl7_btn.click(dq_hl7, outputs=hl7_dq)

        gr.Markdown("### FHIR/OMOP SNOMED Validation")
        snomed_dq = gr.DataFrame(label="SNOMED Quality", interactive=False)
        snomed_btn = gr.Button("Check SNOMED Quality")
        snomed_btn.click(dq_snomed, outputs=snomed_dq)

        gr.Markdown("### CCDA Medication Completeness")
        ccda_dq = gr.DataFrame(label="CCDA Quality", interactive=False)
        ccda_btn = gr.Button("Check CCDA Quality")
        ccda_btn.click(dq_ccda, outputs=ccda_dq)

        gr.Markdown("### JDBC Audit Log")
        audit_df = gr.DataFrame(label="Audit Log", interactive=False)
        audit_btn = gr.Button("Load Audit Log")
        audit_btn.click(dq_audit, outputs=audit_df)


# ---------------------------------------------------------------------------
# Build the app
# ---------------------------------------------------------------------------

with gr.Blocks(
    title="HLS Interoperability Workshop",
    theme=gr.themes.Soft(),
    css=".gradio-container { max-width: 1200px; margin: auto; }",
) as app:
    gr.Markdown(
        f"# Healthcare Interoperability Workshop\n"
        f"Catalog: `{CATALOG}` — Synthetic data only, no PHI"
    )

    with gr.Tabs():
        with gr.Tab("Overview"):
            build_overview_tab()

        with gr.Tab("CCDA Medications"):
            build_ccda_tab()

        with gr.Tab("FHIR / OMOP"):
            build_fhir_tab()

        with gr.Tab("HL7 Admissions"):
            build_hl7_tab()

        with gr.Tab("UC Functions"):
            build_uc_functions_tab()

        with gr.Tab("Data Quality"):
            build_dq_tab()

    gr.Markdown(
        "---\n"
        "Healthcare Interoperability Workshop — Databricks App (Gradio). "
        f"Synthetic data (Synthea). No PHI. Catalog: `{CATALOG}`."
    )

if __name__ == "__main__":
    app.launch(server_name="0.0.0.0", server_port=8080)
