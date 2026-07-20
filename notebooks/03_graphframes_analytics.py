# Databricks notebook source
"""Gold talent graph and structural flight-risk analytics.

Databricks cluster requirement: install a GraphFrames package compatible with
the cluster Spark/Scala runtime before running this notebook.
"""

from graphframes import GraphFrame
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("invisible-skill-matrix-gold").getOrCreate()

CATALOG = "hive_metastore"
SCHEMA = "talent_intelligence"
BRONZE_EMPLOYEES = f"{CATALOG}.{SCHEMA}.bronze_hr_employees"
SILVER_SKILLS = f"{CATALOG}.{SCHEMA}.silver_skill_taxonomy"
SILVER_EMPLOYEE_SKILLS = f"{CATALOG}.{SCHEMA}.silver_employee_skills"
GOLD_RISK_MATRIX = f"{CATALOG}.{SCHEMA}.gold_critical_skill_flight_risk"

employees = spark.table(BRONZE_EMPLOYEES)
skills = spark.table(SILVER_SKILLS)
employee_skills = spark.table(SILVER_EMPLOYEE_SKILLS)

employee_vertices = employees.select(
    F.concat(F.lit("EMP::"), F.col("employee_id")).alias("id"),
    F.lit("Employee").alias("vertex_type"),
    "employee_id",
    F.col("employee_name").alias("name"),
    "manager_id",
    "job_title",
    "department",
    "engagement_score",
    F.lit(None).cast("string").alias("standard_name"),
    F.lit(None).cast("string").alias("category"),
    F.lit(None).cast("boolean").alias("critical_skill"),
    F.lit(None).cast("int").alias("business_criticality"),
)

skill_vertices = skills.select(
    F.concat(F.lit("SKILL::"), F.col("skill_id")).alias("id"),
    F.lit("Skill").alias("vertex_type"),
    F.lit(None).cast("string").alias("employee_id"),
    F.lit(None).cast("string").alias("name"),
    F.lit(None).cast("string").alias("manager_id"),
    F.lit(None).cast("string").alias("job_title"),
    F.lit(None).cast("string").alias("department"),
    F.lit(None).cast("double").alias("engagement_score"),
    "standard_name",
    "category",
    "critical_skill",
    "business_criticality",
)

vertices = employee_vertices.unionByName(skill_vertices)

has_skill_edges = employee_skills.select(
    F.concat(F.lit("EMP::"), F.col("employee_id")).alias("src"),
    F.concat(F.lit("SKILL::"), F.col("skill_id")).alias("dst"),
    F.lit("HAS_SKILL").alias("edge_type"),
    "proficiency",
    "source_system",
    "observed_at",
)

reports_to_edges = employees.filter(F.col("manager_id").isNotNull()).select(
    F.concat(F.lit("EMP::"), F.col("employee_id")).alias("src"),
    F.concat(F.lit("EMP::"), F.col("manager_id")).alias("dst"),
    F.lit("REPORTS_TO").alias("edge_type"),
    F.lit(None).cast("int").alias("proficiency"),
    F.lit("HRIS").alias("source_system"),
    F.col("record_updated_at").alias("observed_at"),
)

edges = has_skill_edges.unionByName(reports_to_edges)
graph = GraphFrame(vertices, edges)

# Motif Mining: connect each employee directly to the governed skill they hold.
skill_motif = graph.find("(emp)-[has_skill]->(skill)").filter(
    (F.col("emp.vertex_type") == "Employee")
    & (F.col("has_skill.edge_type") == "HAS_SKILL")
    & (F.col("skill.vertex_type") == "Skill")
)

skill_holder_counts = (
    has_skill_edges.groupBy("dst")
    .agg(F.countDistinct("src").alias("employee_holder_count"))
)

risk_matrix = (
    skill_motif.join(skill_holder_counts, F.col("skill.id") == F.col("dst"), "left")
    .filter(
        (F.col("skill.critical_skill") == True)
        & (F.col("emp.engagement_score") < 3.0)
    )
    .select(
        F.col("emp.employee_id").alias("employee_id"),
        F.col("emp.name").alias("employee_name"),
        F.col("emp.job_title").alias("job_title"),
        F.col("emp.department").alias("department"),
        F.col("emp.engagement_score").alias("engagement_score"),
        F.col("skill.standard_name").alias("critical_skill"),
        F.col("skill.category").alias("skill_category"),
        F.col("skill.business_criticality").alias("business_criticality"),
        F.col("has_skill.proficiency").alias("proficiency"),
        "employee_holder_count",
        (F.col("employee_holder_count") == 1).alias("single_point_of_failure"),
        F.when(F.col("employee_holder_count") == 1, F.lit("CRITICAL"))
        .when(F.col("employee_holder_count") <= 2, F.lit("HIGH"))
        .otherwise(F.lit("ELEVATED"))
        .alias("risk_tier"),
    )
    .orderBy(
        F.col("single_point_of_failure").desc(),
        F.col("business_criticality").desc(),
        F.col("engagement_score").asc(),
    )
)

# Business Logic: flagging a single point of failure enables retention action,
# succession planning, and internal skill transfer before attrition occurs.
(risk_matrix.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_RISK_MATRIX))

print(f"Created governed Gold data product: {GOLD_RISK_MATRIX}")
risk_matrix.show(truncate=False)
