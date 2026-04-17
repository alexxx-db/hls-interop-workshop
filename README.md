# Healthcare Interoperability Workshop

Workshop material for "Architecture & Interoperability, Lakehouse Best Practices."
Updated for **Databricks Runtime 15.x+**, **Unity Catalog**, and **Serverless compute**.

## Notebooks

| Notebook | Pattern | Description |
|----------|---------|-------------|
| `jdbc.py` | JDBC ingestion | Transfer tables from external JDBC sources with audit logging |
| `file.py` | File-based ingestion | Load CDA/CCDA XML clinical documents, extract medications |
| `fhir.py` | FHIR to OMOP | Transform FHIR Bundles to OMOP CDM using dbignite |
| `hl7.py` | HL7v2 parsing | Parse HL7v2 ADT messages using Python, write to Delta |
| `uc_functions.py` | UC functions | Registers reusable SQL functions (code lookups, risk tiers, validation) |
| `dashboard.py` | Dashboard | AI/BI (Lakeview) dashboard tile definitions across all 4 patterns |
| `genie_space.py` | Genie space | AI/BI Genie natural-language interface with instructions and sample questions |
| `deploy_app.py` | App deployment | Deploys the Streamlit Databricks App |
| `app/app.py` | Streamlit app | 6-page interactive app: overview, CCDA, FHIR/OMOP, HL7, UC functions, data quality |
| `RUNME.py` | Orchestrator | Creates a serverless Workflow to run all notebooks |

## Prerequisites

1. **Databricks Runtime:** DBR 15.x+ (Spark 3.5, Python 3.11+, Java 17)
2. **Unity Catalog** enabled
3. **Serverless compute** enabled
4. Permission to create catalogs/schemas, or an existing target catalog

## Setup

1. Import this repo into Databricks Repos
2. Set the `catalog` widget on each notebook to your target catalog
3. Run `RUNME.py` to create a Workflow, or run notebooks individually

## Library Requirements

| Notebook | Library | Install method |
|----------|---------|----------------|
| `file.py` | `com.databricks:spark-xml_2.12:0.17.0` | Workspace library (Maven) |
| `fhir.py` | `dbignite` | Workspace library (wheel from `alexxx-db/dbignite` `dist/`) |
| `hl7.py` | `python-hl7` | `%pip install` in notebook |

## Data Sources

All notebooks use synthetic data from [Synthea](https://synthetichealth.github.io/synthea/).
No real PHI is used anywhere in this workshop.

> Walonoski et al., "Synthea: An approach, method, and software mechanism for
> generating synthetic patients and the synthetic electronic health care record,"
> JAMIA, Volume 25, Issue 3, March 2018, pp. 230-238.
