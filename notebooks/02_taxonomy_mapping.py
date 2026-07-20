# Databricks notebook source
"""Silver taxonomy standardization for employee skill evidence."""

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("invisible-skill-matrix-silver").getOrCreate()

CATALOG = "hive_metastore"
SCHEMA = "talent_intelligence"
BRONZE_SKILL_LOGS = f"{CATALOG}.{SCHEMA}.bronze_skill_extraction_logs"
SILVER_SKILLS = f"{CATALOG}.{SCHEMA}.silver_skill_taxonomy"
SILVER_EMPLOYEE_SKILLS = f"{CATALOG}.{SCHEMA}.silver_employee_skills"

# Small governed reference taxonomy. In production this would be versioned and stewarded.
taxonomy_schema = T.StructType([
    T.StructField("skill_id", T.StringType(), False),
    T.StructField("standard_name", T.StringType(), False),
    T.StructField("category", T.StringType(), False),
    T.StructField("critical_skill", T.BooleanType(), False),
    T.StructField("business_criticality", T.IntegerType(), False),
    T.StructField("match_pattern", T.StringType(), False),
])

taxonomy_rows = [
    ("SK001", "Apache Spark", "Data Engineering", True, 5, r"pyspark|sparksql|apache spark"),
    ("SK002", "Python", "Programming", True, 4, r"python"),
    ("SK003", "Power BI", "Business Intelligence", False, 3, r"power bi|powerbi|dax"),
    ("SK004", "REST API Development", "Software Engineering", False, 3, r"rest api"),
    ("SK005", "MLflow", "Machine Learning Operations", True, 5, r"mlflow"),
    ("SK006", "Delta Lake", "Data Engineering", True, 5, r"delta lake|lakehouse"),
]

taxonomy = spark.createDataFrame(taxonomy_rows, taxonomy_schema)
raw_logs = spark.table(BRONZE_SKILL_LOGS)

normalized_logs = raw_logs.withColumn(
    "normalized_skill_text",
    F.trim(
        F.regexp_replace(
            F.regexp_replace(F.lower(F.col("raw_skill_text")), r"[^a-z0-9]+", " "),
            r"\s+",
            " ",
        )
    ),
)

# Deterministic regex mapping is transparent, fast, and easy for data stewards to audit.
mapped = (
    normalized_logs.crossJoin(F.broadcast(taxonomy))
    .filter(F.col("normalized_skill_text").rlike(F.col("match_pattern")))
    .select(
        "event_id",
        "employee_id",
        "source_system",
        "raw_skill_text",
        "normalized_skill_text",
        "proficiency",
        "observed_at",
        "skill_id",
        "standard_name",
        "category",
        "critical_skill",
        "business_criticality",
    )
)

# Keep the strongest and most recent evidence for each employee-skill pair.
dedupe_window = Window.partitionBy("employee_id", "skill_id").orderBy(
    F.col("proficiency").desc(), F.col("observed_at").desc(), F.col("event_id").desc()
)

silver_employee_skills = (
    mapped.withColumn("evidence_rank", F.row_number().over(dedupe_window))
    .filter(F.col("evidence_rank") == 1)
    .drop("evidence_rank")
)

(taxonomy.drop("match_pattern").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_SKILLS))
(silver_employee_skills.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_EMPLOYEE_SKILLS))

# Business Logic: a standardized matrix makes internal talent searchable before external hiring begins.
print(f"Created {SILVER_SKILLS} and {SILVER_EMPLOYEE_SKILLS}")
silver_employee_skills.orderBy("employee_id", "standard_name").show(truncate=False)
