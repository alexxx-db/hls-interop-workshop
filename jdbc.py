# Databricks notebook source
# COMPUTE: Serverless

# MAGIC %md
# MAGIC # JDBC Ingestion Pattern
# MAGIC
# MAGIC Demonstrates transferring tables from an external JDBC source into Unity Catalog
# MAGIC managed tables with audit logging. Designed for self-service: a small analytics
# MAGIC team can onboard new data sources without custom ETL code.
# MAGIC
# MAGIC **Synthetic data only — no PHI.**

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "hls_workshop", "Target Catalog")
dbutils.widgets.text("schema", "ingest", "Target Schema")
# JDBC connection — fill in before running
dbutils.widgets.text("jdbc_url", "", "JDBC URL")
dbutils.widgets.text("jdbc_driver", "com.simba.spark.jdbc.Driver", "JDBC Driver Class")
dbutils.widgets.text("source_table", "provider_db.nppes_provider", "Source Table (JDBC)")
dbutils.widgets.text("target_table", "nppes_provider", "Target Table Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ql = f"`{catalog}`.`{schema}`"

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ql}")

# Audit schema for tracking ingestion history
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`audit`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## JDBC Transfer Function
# MAGIC
# MAGIC Reads a table via JDBC and writes it as a Unity Catalog managed Delta table.
# MAGIC Returns an audit record with row count, timing, and source metadata.

# COMMAND ----------

import time
from datetime import date


def transfer_table(
    jdbc_url: str,
    jdbc_driver: str,
    source_table: str,
    target_table: str,
) -> dict:
    """Transfer a JDBC source table to a UC managed Delta table.

    Returns an audit dict with row count, timing, and source metadata.
    The JDBC URL is truncated in the audit record to avoid leaking credentials.
    """
    timer_start = time.time()

    df = (
        spark.read.format("jdbc")
        .option("driver", jdbc_driver)
        .option("url", jdbc_url)
        .option("dbtable", source_table)
        .load()
    )

    target_fqn = f"{ql}.`{target_table}`"
    df.write.format("delta").mode("overwrite").saveAsTable(target_fqn)

    timer_end = time.time()
    return {
        "row_count": df.count(),
        "time_seconds": f"{timer_end - timer_start:0.4f}",
        "source_table": source_table,
        "target_table": target_fqn,
        "jdbc_source": jdbc_url[:95] + "..." if len(jdbc_url) > 95 else jdbc_url,
        "refresh_date": str(date.today()),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Transfer
# MAGIC
# MAGIC Fill in the `jdbc_url` widget before running this cell. The JDBC driver JAR
# MAGIC must be installed as a workspace library.

# COMMAND ----------

# DBTITLE 1,Transfer table via JDBC
jdbc_url = dbutils.widgets.get("jdbc_url")
jdbc_driver = dbutils.widgets.get("jdbc_driver")
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")

if not jdbc_url:
    print("Set the jdbc_url widget before running. Skipping transfer.")
    result = {
        "row_count": 0,
        "time_seconds": "0",
        "source_table": source_table,
        "target_table": f"{ql}.`{target_table}`",
        "jdbc_source": "(not configured)",
        "refresh_date": str(date.today()),
    }
else:
    result = transfer_table(jdbc_url, jdbc_driver, source_table, target_table)
    print(f"Transferred {result['row_count']} rows in {result['time_seconds']}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Log
# MAGIC
# MAGIC Every transfer is logged to an audit table. This provides transparency for
# MAGIC data quality and governance — when was the data last refreshed, how many rows,
# MAGIC from which source.

# COMMAND ----------

# DBTITLE 1,Append audit record
audit_table = f"`{catalog}`.`audit`.`jdbc_ingests`"
spark.createDataFrame([result]).write.format("delta").mode("append").saveAsTable(audit_table)

display(spark.sql(f"SELECT * FROM {audit_table} ORDER BY refresh_date DESC LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Considerations for scaling
# MAGIC
# MAGIC 1. **Database-specific parameters:** e.g., SQL Server `ISOLATION_LEVEL = READ UNCOMMITTED`
# MAGIC    to avoid blocking the source system during extraction.
# MAGIC 2. **Parallelism:** Use `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound`
# MAGIC    for large table pulls.
# MAGIC 3. **Incremental loads:** Use a watermark column (e.g., `last_modified`) with MERGE
# MAGIC    instead of full overwrite.
