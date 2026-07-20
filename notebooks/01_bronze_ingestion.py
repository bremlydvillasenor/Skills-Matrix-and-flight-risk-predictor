# Databricks notebook source
"""Bronze ingestion for the Invisible Skill Matrix portfolio project.

Creates intentionally messy synthetic HR and skill-extraction records, then
persists them as Delta tables. The script is idempotent and safe to rerun.
"""

from pyspark.sql import SparkSession, types as T

spark = SparkSession.builder.appName("invisible-skill-matrix-bronze").getOrCreate()

CATALOG = "hive_metastore"
SCHEMA = "talent_intelligence"
BRONZE_EMPLOYEES = f"{CATALOG}.{SCHEMA}.bronze_hr_employees"
BRONZE_SKILL_LOGS = f"{CATALOG}.{SCHEMA}.bronze_skill_extraction_logs"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

employee_schema = T.StructType([
    T.StructField("employee_id", T.StringType(), False),
    T.StructField("employee_name", T.StringType(), False),
    T.StructField("manager_id", T.StringType(), True),
    T.StructField("job_title", T.StringType(), False),
    T.StructField("department", T.StringType(), False),
    T.StructField("engagement_score", T.DoubleType(), False),
    T.StructField("record_updated_at", T.TimestampType(), False),
])

employee_rows = [
    ("E001", "Ava Santos", None, "VP Data", "Data & AI", 4.4, "2026-07-01 09:00:00"),
    ("E002", "Noah Reyes", "E001", "Lead Data Engineer", "Data & AI", 2.4, "2026-07-01 09:05:00"),
    ("E003", "Mia Cruz", "E002", "Analytics Engineer", "Data & AI", 4.1, "2026-07-01 09:10:00"),
    ("E004", "Liam Garcia", "E002", "Backend Engineer", "Engineering", 2.7, "2026-07-01 09:15:00"),
    ("E005", "Zoe Mendoza", "E001", "BI Developer", "People Analytics", 3.8, "2026-07-01 09:20:00"),
    ("E006", "Ethan Lim", "E001", "ML Engineer", "Data & AI", 2.2, "2026-07-01 09:25:00"),
]

employees = spark.createDataFrame(employee_rows, schema=employee_schema)

skill_log_schema = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("employee_id", T.StringType(), False),
    T.StructField("source_system", T.StringType(), False),
    T.StructField("raw_skill_text", T.StringType(), False),
    T.StructField("proficiency", T.IntegerType(), False),
    T.StructField("observed_at", T.TimestampType(), False),
])

skill_rows = [
    ("S001", "E002", "JIRA", "PySpark 3.x", 5, "2026-06-20 10:00:00"),
    ("S002", "E002", "RESUME", "SparkSQL", 5, "2026-06-21 10:00:00"),
    ("S003", "E003", "JIRA", "python backend", 4, "2026-06-22 10:00:00"),
    ("S004", "E003", "RESUME", "Power BI", 4, "2026-06-23 10:00:00"),
    ("S005", "E004", "JIRA", "python-backend", 5, "2026-06-24 10:00:00"),
    ("S006", "E004", "RESUME", "REST APIs", 4, "2026-06-25 10:00:00"),
    ("S007", "E005", "JIRA", "powerbi dax", 5, "2026-06-26 10:00:00"),
    ("S008", "E006", "RESUME", "MLflow tracking", 5, "2026-06-27 10:00:00"),
    ("S009", "E006", "JIRA", "Apache-Spark", 4, "2026-06-28 10:00:00"),
    ("S010", "E002", "JIRA", "delta lakehouse", 5, "2026-06-29 10:00:00"),
]

skill_logs = spark.createDataFrame(skill_rows, schema=skill_log_schema)

# Bronze preserves source-system language and imperfections for auditability.
(employees.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_EMPLOYEES))
(skill_logs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_SKILL_LOGS))

employees.createOrReplaceTempView("bronze_hr_employees")
skill_logs.createOrReplaceTempView("bronze_skill_extraction_logs")

print(f"Created {BRONZE_EMPLOYEES} and {BRONZE_SKILL_LOGS}")
employees.show(truncate=False)
skill_logs.show(truncate=False)
