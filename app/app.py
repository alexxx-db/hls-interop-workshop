"""
Healthcare Interoperability Workshop — Databricks App (Streamlit)

A unified interface for exploring the four interoperability patterns
demonstrated in the workshop: JDBC, CDA/CCDA XML, FHIR/OMOP, and HL7v2.

Runs as a Databricks App on Serverless compute. Connects to the workshop
catalog via the Databricks SQL Connector.

Synthetic data only — no PHI.
"""

import os
import streamlit as st
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

# Databricks Apps inject connection details automatically;
# fall back to env vars for local development.


@st.cache_resource
def get_connection():
    """Create a reusable Databricks SQL connection."""
    return dbsql.connect(
        server_hostname=HOST,
        http_path=WAREHOUSE_HTTP_PATH,
        access_token=TOKEN,
    )


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
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="HLS Interoperability Workshop",
    page_icon="🏥",
    layout="wide",
)

st.title("Healthcare Interoperability Workshop")
st.caption(f"Catalog: `{CATALOG}` — Synthetic data only, no PHI")

# Sidebar navigation
page = st.sidebar.radio(
    "Navigate",
    [
        "Overview",
        "CCDA Medications",
        "FHIR / OMOP",
        "HL7 Admissions",
        "UC Functions Explorer",
        "Data Quality",
    ],
)

# ---------------------------------------------------------------------------
# Page: Overview
# ---------------------------------------------------------------------------

if page == "Overview":
    st.header("Workshop Data Summary")
    st.markdown(
        """
        This workshop demonstrates four healthcare data integration patterns:
        **JDBC ingestion**, **CDA/CCDA XML parsing**, **FHIR-to-OMOP transformation**,
        and **HL7v2 ADT message parsing**. Each pattern produces tables in its own
        schema within the Unity Catalog.
        """
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        df = safe_query(f"SELECT count(*) AS cnt FROM `{CATALOG}`.`ccda`.`ccda_claims_raw`")
        val = int(df["cnt"].iloc[0]) if not df.empty else 0
        st.metric("CCDA Documents", f"{val:,}")

    with col2:
        df = safe_query(f"SELECT count(*) AS cnt FROM `{CATALOG}`.`ccda`.`medications`")
        val = int(df["cnt"].iloc[0]) if not df.empty else 0
        st.metric("Medications Extracted", f"{val:,}")

    with col3:
        df = safe_query(f"SELECT count(*) AS cnt FROM `{CATALOG}`.`fhir`.`person`")
        val = int(df["cnt"].iloc[0]) if not df.empty else 0
        st.metric("OMOP Patients", f"{val:,}")

    with col4:
        df = safe_query(f"SELECT count(*) AS cnt FROM `{CATALOG}`.`hl7`.`parsed_hl7`")
        val = int(df["cnt"].iloc[0]) if not df.empty else 0
        st.metric("HL7 Messages", f"{val:,}")

    st.divider()

    st.subheader("Table Inventory")
    inventory = safe_query(f"""
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
    if not inventory.empty:
        st.dataframe(inventory, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page: CCDA Medications
# ---------------------------------------------------------------------------

elif page == "CCDA Medications":
    st.header("CDA/CCDA — Medication Analysis")
    st.markdown("Medications extracted from Synthea CCDA XML clinical documents.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Medications")
        meds = safe_query(f"""
            SELECT medication_name, count(*) AS patients
            FROM `{CATALOG}`.`ccda`.`medications`
            WHERE medication_name IS NOT NULL
            GROUP BY medication_name
            ORDER BY patients DESC
            LIMIT 15
        """)
        if not meds.empty:
            st.bar_chart(meds.set_index("medication_name"), horizontal=True)

    with col2:
        st.subheader("Prescriptions by City")
        cities = safe_query(f"""
            SELECT city, count(*) AS prescriptions
            FROM `{CATALOG}`.`ccda`.`medications`
            WHERE city IS NOT NULL
            GROUP BY city
            ORDER BY prescriptions DESC
            LIMIT 10
        """)
        if not cities.empty:
            st.bar_chart(cities.set_index("city"))

    st.divider()
    st.subheader("Medication Records")
    limit = st.slider("Rows to display", 10, 200, 50, key="ccda_limit")
    records = safe_query(f"""
        SELECT city, first_name, last_name, medication_name
        FROM `{CATALOG}`.`ccda`.`medications`
        LIMIT {limit}
    """)
    if not records.empty:
        st.dataframe(records, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page: FHIR / OMOP
# ---------------------------------------------------------------------------

elif page == "FHIR / OMOP":
    st.header("FHIR-to-OMOP — Clinical Profile")
    st.markdown("FHIR R4 Bundles transformed into OMOP CDM tables using dbignite.")

    # Condition prevalence
    st.subheader("Chronic Condition Prevalence")
    prevalence = safe_query(f"""
        SELECT 'Diabetes' AS condition, sum(diabetes_ind) AS patients
        FROM `{CATALOG}`.`fhir`.`condition_features`
        UNION ALL
        SELECT 'Hypertension', sum(hypertension_ind) FROM `{CATALOG}`.`fhir`.`condition_features`
        UNION ALL
        SELECT 'Substance Use', sum(substance_use_ind) FROM `{CATALOG}`.`fhir`.`condition_features`
    """)
    if not prevalence.empty:
        st.bar_chart(prevalence.set_index("condition"))

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("OMOP Table Sizes")
        omop_sizes = safe_query(f"""
            SELECT 'person' AS table_name, count(*) AS rows FROM `{CATALOG}`.`fhir`.`person`
            UNION ALL SELECT 'condition_occurrence', count(*) FROM `{CATALOG}`.`fhir`.`condition_occurrence`
            UNION ALL SELECT 'procedure_occurrence', count(*) FROM `{CATALOG}`.`fhir`.`procedure_occurrence`
            UNION ALL SELECT 'visit_occurrence', count(*) FROM `{CATALOG}`.`fhir`.`visit_occurrence`
        """)
        if not omop_sizes.empty:
            st.bar_chart(omop_sizes.set_index("table_name"))

    with col2:
        st.subheader("Comorbidity Distribution")
        comorbidity = safe_query(f"""
            SELECT
                CASE
                    WHEN diabetes_ind + hypertension_ind + substance_use_ind = 0 THEN 'No conditions'
                    WHEN diabetes_ind + hypertension_ind + substance_use_ind = 1 THEN '1 condition'
                    WHEN diabetes_ind + hypertension_ind + substance_use_ind = 2 THEN '2 conditions'
                    ELSE '3 conditions'
                END AS comorbidity,
                count(*) AS patients
            FROM `{CATALOG}`.`fhir`.`condition_features`
            GROUP BY 1 ORDER BY 1
        """)
        if not comorbidity.empty:
            st.bar_chart(comorbidity.set_index("comorbidity"))

    st.divider()
    st.subheader("Condition Records (with UC function enrichment)")
    conditions = safe_query(f"""
        SELECT
            person_id,
            condition_source_value AS code,
            `{CATALOG}`.`functions`.`snomed_description`(condition_source_value) AS description,
            `{CATALOG}`.`functions`.`is_valid_snomed`(condition_source_value) AS valid_snomed
        FROM `{CATALOG}`.`fhir`.`condition_occurrence`
        LIMIT 50
    """)
    if not conditions.empty:
        st.dataframe(conditions, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page: HL7 Admissions
# ---------------------------------------------------------------------------

elif page == "HL7 Admissions":
    st.header("HL7v2 — ADT Admission Analysis")
    st.markdown("Parsed HL7v2 ADT messages from member hospital feeds.")

    # Filters
    facilities = safe_query(f"""
        SELECT DISTINCT facility FROM `{CATALOG}`.`hl7`.`parsed_hl7`
        WHERE facility IS NOT NULL ORDER BY facility
    """)
    facility_options = ["All"] + (facilities["facility"].tolist() if not facilities.empty else [])
    selected_facility = st.selectbox("Filter by Facility", facility_options)

    where_clause = ""
    if selected_facility != "All":
        where_clause = f"WHERE facility = '{selected_facility}'"

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Admissions by Facility")
        by_facility = safe_query(f"""
            SELECT facility, count(*) AS admissions
            FROM `{CATALOG}`.`hl7`.`parsed_hl7`
            {where_clause}
            GROUP BY facility ORDER BY admissions DESC
        """)
        if not by_facility.empty:
            st.bar_chart(by_facility.set_index("facility"))

    with col2:
        st.subheader("Top Admit Diagnoses")
        by_dx = safe_query(f"""
            SELECT
                admit_diagnosis AS code,
                `{CATALOG}`.`functions`.`icd10_description`(admit_diagnosis) AS diagnosis,
                count(*) AS frequency
            FROM `{CATALOG}`.`hl7`.`parsed_hl7`
            {where_clause}
            WHERE admit_diagnosis IS NOT NULL AND admit_diagnosis != ''
            GROUP BY admit_diagnosis ORDER BY frequency DESC
            LIMIT 10
        """)
        if not by_dx.empty:
            st.bar_chart(by_dx.set_index("diagnosis")["frequency"], horizontal=True)

    st.divider()
    st.subheader("Recent HL7 Messages")
    limit = st.slider("Rows to display", 10, 100, 20, key="hl7_limit")
    messages = safe_query(f"""
        SELECT
            patient_id, facility, message_type,
            admit_diagnosis,
            `{CATALOG}`.`functions`.`icd10_description`(admit_diagnosis) AS diagnosis_name,
            `{CATALOG}`.`functions`.`adt_event_description`(
                regexp_extract(message_type, 'A[0-9]+', 0)
            ) AS event_description,
            parsed_at
        FROM `{CATALOG}`.`hl7`.`parsed_hl7`
        {where_clause}
        ORDER BY parsed_at DESC
        LIMIT {limit}
    """)
    if not messages.empty:
        st.dataframe(messages, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page: UC Functions Explorer
# ---------------------------------------------------------------------------

elif page == "UC Functions Explorer":
    st.header("Unity Catalog Functions")
    st.markdown(
        """
        These reusable SQL functions are registered in Unity Catalog and available
        to notebooks, dashboards, Genie, Tableau, and any JDBC client.
        Try them below — enter a code and see the result.
        """
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("SNOMED Lookup")
        snomed_code = st.text_input("SNOMED code", "44054006", key="snomed")
        if snomed_code:
            result = safe_query(
                f"SELECT `{CATALOG}`.`functions`.`snomed_description`('{snomed_code}') AS description"
            )
            if not result.empty:
                st.success(result["description"].iloc[0])

    with col2:
        st.subheader("ICD-10 Lookup")
        icd_code = st.text_input("ICD-10-CM code", "I10", key="icd10")
        if icd_code:
            result = safe_query(
                f"SELECT `{CATALOG}`.`functions`.`icd10_description`('{icd_code}') AS description"
            )
            if not result.empty:
                st.success(result["description"].iloc[0])

    with col3:
        st.subheader("ADT Event Lookup")
        adt_code = st.text_input("ADT event code", "A01", key="adt")
        if adt_code:
            result = safe_query(
                f"SELECT `{CATALOG}`.`functions`.`adt_event_description`('{adt_code}') AS description"
            )
            if not result.empty:
                st.success(result["description"].iloc[0])

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Risk Tier Classification")
        condition_count = st.number_input("Chronic condition count", 0, 5, 2, key="risk")
        result = safe_query(
            f"SELECT `{CATALOG}`.`functions`.`chronic_risk_tier`({condition_count}) AS tier"
        )
        if not result.empty:
            tier = result["tier"].iloc[0]
            color = {"high": "🔴", "moderate": "🟡", "low": "🟢", "none": "⚪"}.get(tier, "")
            st.info(f"{color} Risk tier: **{tier}**")

    with col2:
        st.subheader("Code Validation")
        test_code = st.text_input("Code to validate", "44054006", key="validate")
        if test_code:
            result = safe_query(f"""
                SELECT
                    `{CATALOG}`.`functions`.`is_valid_snomed`('{test_code}') AS valid_snomed,
                    `{CATALOG}`.`functions`.`is_valid_icd10`('{test_code}') AS valid_icd10
            """)
            if not result.empty:
                row = result.iloc[0]
                st.write(f"Valid SNOMED: {'Yes' if row['valid_snomed'] else 'No'}")
                st.write(f"Valid ICD-10: {'Yes' if row['valid_icd10'] else 'No'}")

    st.divider()
    st.subheader("All Registered Functions")
    functions = safe_query(f"SHOW FUNCTIONS IN `{CATALOG}`.`functions`")
    if not functions.empty:
        st.dataframe(functions, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Page: Data Quality
# ---------------------------------------------------------------------------

elif page == "Data Quality":
    st.header("Data Quality Overview")
    st.markdown("Null rates and validation checks across workshop tables.")

    st.subheader("HL7 Parse Quality")
    hl7_quality = safe_query(f"""
        SELECT
            count(*) AS total_messages,
            sum(CASE WHEN patient_id IS NULL OR patient_id = '' THEN 1 ELSE 0 END) AS missing_patient_id,
            sum(CASE WHEN admit_diagnosis IS NULL OR admit_diagnosis = '' THEN 1 ELSE 0 END) AS missing_diagnosis,
            sum(CASE WHEN facility IS NULL OR facility = '' THEN 1 ELSE 0 END) AS missing_facility,
            round(
                sum(CASE WHEN patient_id IS NOT NULL AND patient_id != '' THEN 1 ELSE 0 END) * 100.0 / count(*), 1
            ) AS patient_id_completeness_pct
        FROM `{CATALOG}`.`hl7`.`parsed_hl7`
    """)
    if not hl7_quality.empty:
        row = hl7_quality.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Messages", f"{int(row['total_messages']):,}")
        col2.metric("Missing Patient ID", int(row["missing_patient_id"]))
        col3.metric("Missing Diagnosis", int(row["missing_diagnosis"]))
        col4.metric("Patient ID Completeness", f"{row['patient_id_completeness_pct']}%")

    st.divider()

    st.subheader("FHIR/OMOP Condition Validation")
    snomed_quality = safe_query(f"""
        SELECT
            count(*) AS total_conditions,
            sum(CASE WHEN `{CATALOG}`.`functions`.`is_valid_snomed`(condition_source_value) THEN 1 ELSE 0 END) AS valid_snomed_codes,
            round(
                sum(CASE WHEN `{CATALOG}`.`functions`.`is_valid_snomed`(condition_source_value) THEN 1 ELSE 0 END) * 100.0 / count(*), 1
            ) AS snomed_valid_pct
        FROM `{CATALOG}`.`fhir`.`condition_occurrence`
    """)
    if not snomed_quality.empty:
        row = snomed_quality.iloc[0]
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Conditions", f"{int(row['total_conditions']):,}")
        col2.metric("Valid SNOMED Codes", f"{int(row['valid_snomed_codes']):,}")
        col3.metric("SNOMED Validity Rate", f"{row['snomed_valid_pct']}%")

    st.divider()

    st.subheader("CCDA Medication Completeness")
    ccda_quality = safe_query(f"""
        SELECT
            count(*) AS total_records,
            sum(CASE WHEN medication_name IS NULL THEN 1 ELSE 0 END) AS null_medication_name,
            sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS null_city,
            sum(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) AS null_first_name
        FROM `{CATALOG}`.`ccda`.`medications`
    """)
    if not ccda_quality.empty:
        row = ccda_quality.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Records", f"{int(row['total_records']):,}")
        col2.metric("Null Medication Name", int(row["null_medication_name"]))
        col3.metric("Null City", int(row["null_city"]))
        col4.metric("Null First Name", int(row["null_first_name"]))

    st.divider()

    st.subheader("Audit Log")
    audit = safe_query(f"""
        SELECT * FROM `{CATALOG}`.`audit`.`jdbc_ingests`
        ORDER BY refresh_date DESC
        LIMIT 20
    """)
    if not audit.empty:
        st.dataframe(audit, use_container_width=True, hide_index=True)
    else:
        st.info("No JDBC audit records yet — run the jdbc.py notebook first.")


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption(
    "Healthcare Interoperability Workshop — Databricks App. "
    "Synthetic data (Synthea). No PHI. "
    f"Connected to catalog `{CATALOG}`."
)
