# Databricks notebook source
# COMPUTE: Serverless

# MAGIC %md
# MAGIC # Healthcare Interoperability Workshop — Agent Bricks
# MAGIC
# MAGIC Creates three Agent Bricks and one Supervisor Agent that ties them together:
# MAGIC
# MAGIC 1. **Genie Space** — natural language SQL over the workshop tables
# MAGIC 2. **Knowledge Assistant** — RAG over CCDA clinical documents and workshop docs
# MAGIC 3. **Supervisor Agent** — routes questions to the right specialist
# MAGIC
# MAGIC The Supervisor Agent also has direct access to UC functions for clinical code
# MAGIC lookups (SNOMED, ICD-10, ADT events) without needing to route through Genie.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - All workshop notebooks have been run (tables exist)
# MAGIC - UC functions registered (`uc_functions.py`)
# MAGIC - CCDA XML files in Volume (`file.py`)
# MAGIC
# MAGIC ---
# MAGIC **Synthetic data only — no PHI.**

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "hls_workshop", "Workshop Catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the Genie Space
# MAGIC
# MAGIC The Genie Space enables natural-language SQL queries against all workshop tables.
# MAGIC Users can ask "How many patients have diabetes?" and Genie translates it to SQL
# MAGIC against the FHIR/OMOP tables.

# COMMAND ----------

# DBTITLE 1,Create Genie Space for Workshop Data
genie_tables = [
    # CCDA
    f"{catalog}.ccda.ccda_claims_raw",
    f"{catalog}.ccda.medications",
    # FHIR / OMOP
    f"{catalog}.fhir.person",
    f"{catalog}.fhir.condition_occurrence",
    f"{catalog}.fhir.procedure_occurrence",
    f"{catalog}.fhir.visit_occurrence",
    f"{catalog}.fhir.condition_features",
    # HL7
    f"{catalog}.hl7.parsed_hl7",
    # Audit
    f"{catalog}.audit.jdbc_ingests",
]

genie_instructions = f"""
You are a data assistant for a healthcare interoperability workshop. The workshop
demonstrates four data integration patterns using synthetic (Synthea) data.

Available schemas in `{catalog}`:
- `ccda`: CDA/CCDA clinical documents and extracted medications
- `fhir`: FHIR-to-OMOP transformation output (person, conditions, procedures, visits)
- `hl7`: Parsed HL7v2 ADT admission messages
- `audit`: JDBC ingestion audit log

UC functions available in `{catalog}.functions`:
- snomed_description(code) — translates SNOMED codes
- icd10_description(code) — translates ICD-10 codes
- adt_event_description(code) — translates ADT events
- chronic_risk_tier(count) — classifies comorbidity burden
- is_valid_snomed(code) / is_valid_icd10(code) — code format validation

Use these functions in your SQL when the user asks about diagnosis names or code meanings.

All data is synthetic. No real PHI.
"""

# COMMAND ----------

# DBTITLE 1,Create or update the Genie Space

# The Genie space may already exist from genie_space.py — find it first
genie_space_id = None

try:
    existing = manage_genie(action="list")  # noqa: F821 — MCP tool
    for space in existing.get("spaces", []):
        if "interoperability" in space.get("name", "").lower():
            genie_space_id = space["space_id"]
            print(f"Found existing Genie space: {space['name']} ({genie_space_id})")
            break
except Exception:
    pass

if genie_space_id is None:
    result = manage_genie(  # noqa: F821
        action="create_or_update",
        name="HLS Interoperability Workshop",
        tables=genie_tables,
        instructions=genie_instructions,
        sample_questions=[
            "How many patients have diabetes?",
            "What are the most common medications?",
            "Which facility has the most HL7 admissions?",
            "Show me the top 10 admit diagnoses with their descriptions",
            "How many OMOP conditions are valid SNOMED codes?",
        ],
    )
    genie_space_id = result.get("space_id")
    print(f"Created Genie space: {genie_space_id}")
else:
    print(f"Using existing Genie space: {genie_space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Knowledge Assistant
# MAGIC
# MAGIC The Knowledge Assistant provides RAG-based Q&A over two types of documents:
# MAGIC
# MAGIC 1. **CCDA clinical documents** — the XML files loaded by `file.py`
# MAGIC 2. **Workshop documentation** — README, notebook descriptions
# MAGIC
# MAGIC This lets users ask questions like "What medications is patient John Doe on?"
# MAGIC by searching the actual clinical document content, not just the extracted tables.

# COMMAND ----------

# DBTITLE 1,Prepare KA source documents in Volume
import json

# The KA needs documents in a Volume. The CCDA XMLs are already there from file.py.
# We also generate a workshop FAQ document to answer questions about the workshop itself.
ka_volume = f"/Volumes/{catalog}/ccda/raw_files"

# Generate a workshop FAQ as a supplementary document
faq_content = """
# Healthcare Interoperability Workshop — FAQ

## What is this workshop?
This workshop demonstrates four healthcare data integration patterns on Databricks:
JDBC ingestion, CDA/CCDA XML parsing, FHIR-to-OMOP transformation, and HL7v2 ADT parsing.

## What data is used?
All data is synthetic, generated by Synthea. No real patient data is present.

## What patterns are covered?

### JDBC (jdbc.py)
Transfers tables from external databases via JDBC with audit logging.

### CDA/CCDA XML (file.py)
Loads clinical XML documents, extracts medication data using spark-xml.

### FHIR to OMOP (fhir.py)
Transforms FHIR R4 bundles into OMOP CDM tables (person, condition, procedure, visit)
using the dbignite library. Also builds chronic condition feature indicators.

### HL7v2 ADT (hl7.py)
Parses HL7v2 ADT admission messages using python-hl7. Extracts patient ID,
facility, admit diagnosis, and event type.

## What UC functions are available?
- snomed_description(code): Translates SNOMED CT codes to readable names
- icd10_description(code): Translates ICD-10-CM codes
- adt_event_description(code): Translates HL7 ADT event codes (A01=admitted, etc.)
- age_from_dob(date): Computes age from date of birth
- chronic_risk_tier(count): Classifies comorbidity burden (high/moderate/low/none)
- days_overdue_bucket(days): Categorizes engagement status
- hedis_measure_name(code): Expands HEDIS measure codes
- is_valid_snomed(code): Validates SNOMED code format
- is_valid_icd10(code): Validates ICD-10-CM code format

## What are the OMOP tables?
- person: Patient demographics
- condition_occurrence: Diagnoses with SNOMED codes
- procedure_occurrence: Procedures performed
- visit_occurrence: Encounter/visit records
- condition_features: Per-patient chronic condition flags (diabetes, hypertension, substance use)
"""

faq_path = f"{ka_volume}/workshop_faq.md"
dbutils.fs.put(faq_path, faq_content, overwrite=True)
print(f"Workshop FAQ written to {faq_path}")

# Generate example Q&A pairs for the KA to learn from
examples = [
    {
        "question": "What interoperability patterns does this workshop cover?",
        "guideline": "The workshop covers four patterns: JDBC ingestion, CDA/CCDA XML parsing, FHIR-to-OMOP transformation, and HL7v2 ADT message parsing."
    },
    {
        "question": "What is the OMOP Common Data Model?",
        "guideline": "OMOP CDM is a standardized schema for observational health data. The workshop transforms FHIR bundles into four OMOP tables: person, condition_occurrence, procedure_occurrence, and visit_occurrence."
    },
    {
        "question": "How do I look up what a SNOMED code means?",
        "guideline": "Use the UC function snomed_description(code). For example, snomed_description('44054006') returns 'Diabetes mellitus type 2'."
    },
    {
        "question": "What is an HL7 ADT_A01 message?",
        "guideline": "ADT_A01 is an HL7v2 message indicating a new patient admission. The workshop parses these messages to extract patient ID, facility, and admit diagnosis."
    },
    {
        "question": "Is this real patient data?",
        "guideline": "No. All data is synthetic, generated by Synthea. No real patient health information (PHI) is present."
    },
]

examples_path = f"{ka_volume}/workshop_examples.json"
dbutils.fs.put(examples_path, json.dumps(examples, indent=2), overwrite=True)
print(f"KA examples written to {examples_path}")

# COMMAND ----------

# DBTITLE 1,Create Knowledge Assistant
ka_result = manage_ka(  # noqa: F821 — MCP tool
    action="create_or_update",
    name="HLS Workshop Knowledge Base",
    volume_path=ka_volume,
    description=(
        "Answers questions about the Healthcare Interoperability Workshop, "
        "including the four data integration patterns (JDBC, CCDA, FHIR/OMOP, HL7), "
        "the OMOP data model, UC functions, and clinical document content."
    ),
    instructions=(
        "You are a knowledgeable assistant for a healthcare data interoperability workshop. "
        "Answer questions about the workshop patterns, data models, and clinical content. "
        "All data is synthetic (Synthea-generated) — remind users of this if they ask about "
        "real patients. When asked about clinical codes, mention that UC functions like "
        "snomed_description() and icd10_description() are available for lookups."
    ),
    add_examples_from_volume=True,
)

ka_tile_id = ka_result.get("tile_id")
print(f"Knowledge Assistant created: {ka_tile_id}")
print(f"Endpoint status: {ka_result.get('endpoint_status', 'provisioning...')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the Supervisor Agent
# MAGIC
# MAGIC The Supervisor Agent routes questions to the right specialist:
# MAGIC
# MAGIC - **Data questions** (counts, trends, comparisons) → Genie Space (SQL)
# MAGIC - **Document/knowledge questions** (what is OMOP? how does HL7 work?) → Knowledge Assistant
# MAGIC - **Code lookups** (what does SNOMED 44054006 mean?) → UC function directly
# MAGIC
# MAGIC This creates a single conversational interface that handles all workshop queries.

# COMMAND ----------

# DBTITLE 1,Create Supervisor Agent
agents_config = [
    {
        "name": "data_analyst",
        "genie_space_id": genie_space_id,
        "description": (
            "Answers data questions by running SQL queries against the workshop tables. "
            "Use for questions about patient counts, medication frequencies, admission "
            "statistics, diagnosis distributions, facility comparisons, and any question "
            "that requires aggregating or filtering data from the ccda, fhir, hl7, or "
            "audit schemas."
        ),
    },
    {
        "name": "knowledge_base",
        "ka_tile_id": ka_tile_id,
        "description": (
            "Answers conceptual and procedural questions about the workshop, healthcare "
            "data standards, and clinical document content. Use for questions like "
            "'What is OMOP?', 'How does HL7 work?', 'What patterns does this workshop "
            "cover?', or 'What medications appear in the CCDA documents?'"
        ),
    },
    {
        "name": "snomed_lookup",
        "uc_function_name": f"{catalog}.functions.snomed_description",
        "description": (
            "Translates SNOMED CT clinical codes to human-readable descriptions. "
            "Use when the user asks 'What does SNOMED code X mean?' or 'Translate "
            "this condition code'. Input: a SNOMED code string like '44054006'."
        ),
    },
    {
        "name": "icd10_lookup",
        "uc_function_name": f"{catalog}.functions.icd10_description",
        "description": (
            "Translates ICD-10-CM diagnosis codes to human-readable descriptions. "
            "Use when the user asks about ICD-10 codes like 'I10' or 'E11.9'. "
            "Common in HL7 ADT messages."
        ),
    },
    {
        "name": "adt_event_lookup",
        "uc_function_name": f"{catalog}.functions.adt_event_description",
        "description": (
            "Explains HL7 ADT event types. Use when the user asks 'What does A01 mean?' "
            "or 'What is an ADT transfer event?'. Input: event code like 'A01'."
        ),
    },
    {
        "name": "risk_classifier",
        "uc_function_name": f"{catalog}.functions.chronic_risk_tier",
        "description": (
            "Classifies a patient's comorbidity burden into a risk tier (high/moderate/low/none) "
            "based on the number of chronic conditions. Use when the user asks "
            "'What risk tier is a patient with 3 conditions?' Input: an integer."
        ),
    },
]

supervisor_result = manage_mas(  # noqa: F821 — MCP tool
    action="create_or_update",
    name="HLS Interoperability Workshop Assistant",
    agents=agents_config,
    description=(
        "A unified conversational assistant for the Healthcare Interoperability Workshop. "
        "Routes data questions to Genie (SQL), knowledge questions to the KA (RAG), "
        "and code lookups directly to UC functions."
    ),
    instructions="""
    Route questions as follows:

    1. **Data questions** (How many? Which facility? Top N? Compare X vs Y?)
       → data_analyst (Genie Space)
       Examples: "How many patients have diabetes?", "Top admit diagnoses"

    2. **Knowledge questions** (What is? How does? Explain? What patterns?)
       → knowledge_base (Knowledge Assistant)
       Examples: "What is OMOP?", "How does HL7 parsing work?"

    3. **Code lookups** (What does code X mean? Translate this code)
       → snomed_lookup, icd10_lookup, or adt_event_lookup depending on code type
       - Numeric 6-18 digits → snomed_lookup
       - Letter + digits (e.g., I10, E11.9) → icd10_lookup
       - A01, A02, etc. → adt_event_lookup

    4. **Risk classification** (What risk tier? How many conditions = what level?)
       → risk_classifier

    If a question spans multiple domains, chain the agents:
    - First get the data (data_analyst), then explain the result (knowledge_base)
    - First look up a code (snomed/icd10), then find patients with that condition (data_analyst)

    Always remind users that all data is synthetic (Synthea-generated).
    """,
    examples=[
        {
            "question": "How many patients have diabetes?",
            "guideline": "Route to data_analyst. Query condition_features table."
        },
        {
            "question": "What does SNOMED code 59621000 mean?",
            "guideline": "Route to snomed_lookup. Returns 'Essential hypertension'."
        },
        {
            "question": "What is the OMOP Common Data Model?",
            "guideline": "Route to knowledge_base. Explain OMOP CDM structure."
        },
        {
            "question": "Which facility has the most admissions and what are the top diagnoses?",
            "guideline": "Route to data_analyst for admission counts, then use icd10_lookup to translate the top diagnosis codes."
        },
        {
            "question": "What risk tier is a patient with 3 chronic conditions?",
            "guideline": "Route to risk_classifier with input 3. Returns 'high'."
        },
    ],
)

supervisor_tile_id = supervisor_result.get("tile_id")
print(f"Supervisor Agent created: {supervisor_tile_id}")
print(f"Endpoint status: {supervisor_result.get('endpoint_status', 'provisioning...')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Provisioning
# MAGIC
# MAGIC Agent Bricks take 2-5 minutes to provision. Check the status below.

# COMMAND ----------

# DBTITLE 1,Check Agent Status
import time

print("Waiting for agents to provision (this may take 2-5 minutes)...\n")

for attempt in range(15):
    ka_status = manage_ka(action="get", tile_id=ka_tile_id)  # noqa: F821
    mas_status = manage_mas(action="get", tile_id=supervisor_tile_id)  # noqa: F821

    ka_ep = ka_status.get("endpoint_status", "UNKNOWN")
    mas_ep = mas_status.get("endpoint_status", "UNKNOWN")

    print(f"  Knowledge Assistant: {ka_ep}")
    print(f"  Supervisor Agent:    {mas_ep}")

    if ka_ep == "ONLINE" and mas_ep == "ONLINE":
        print("\nAll agents are ONLINE and ready.")
        break

    if attempt < 14:
        print("  (waiting 30s...)\n")
        time.sleep(30)
else:
    print("\nAgents are still provisioning. Check the Agent Bricks UI for status.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Bricks Summary
# MAGIC
# MAGIC | Brick | Type | Purpose | ID |
# MAGIC |-------|------|---------|-----|
# MAGIC | HLS Interoperability Workshop | Genie Space | SQL queries on workshop tables | `{genie_space_id}` |
# MAGIC | HLS Workshop Knowledge Base | Knowledge Assistant | RAG over CCDA docs + workshop FAQ | `{ka_tile_id}` |
# MAGIC | HLS Interoperability Workshop Assistant | Supervisor Agent | Routes to Genie, KA, or UC functions | `{supervisor_tile_id}` |
# MAGIC
# MAGIC ### How to use
# MAGIC
# MAGIC 1. **Direct access:** Open each brick from the Agent Bricks UI in the workspace
# MAGIC 2. **Supervisor:** Use the Supervisor Agent for a unified experience — it routes
# MAGIC    to the right specialist automatically
# MAGIC 3. **API:** Each brick has a model serving endpoint — call it programmatically
# MAGIC    from notebooks, apps, or external systems
# MAGIC
# MAGIC ### Example conversations with the Supervisor
# MAGIC
# MAGIC - *"How many patients have both diabetes and hypertension?"* → routes to Genie
# MAGIC - *"What does SNOMED code 88805009 mean?"* → routes to snomed_lookup UC function
# MAGIC - *"Explain the four interoperability patterns in this workshop"* → routes to Knowledge Assistant
# MAGIC - *"Which facility has the highest admission rate and what are the common diagnoses?"*
# MAGIC   → chains Genie (data) + icd10_lookup (code translation)
# MAGIC - *"What risk tier would a patient with COPD, diabetes, and hypertension be in?"*
# MAGIC   → routes to risk_classifier with input 3
