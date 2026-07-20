# The Invisible Skill Matrix & Flight-Risk Predictor

A lightweight Talent Intelligence portfolio project that connects fragmented employee skill evidence with engagement risk to identify critical capability gaps before attrition occurs.

## Business problem

Organizations often know who is employed, but not where critical skills actually sit. Jira activity, resumes, HR records, and project evidence remain separated across systems. When a low-engagement employee is the only holder of a business-critical skill, the organization has a hidden operational and hiring risk.

This project demonstrates how a governed talent graph can answer:

- Which employees hold critical skills?
- Which skills are concentrated in one or two employees?
- Which critical skill holders have elevated flight risk?
- Where should leaders prioritize retention, succession, or skill transfer?

## Architecture

```text
Raw HR and skill evidence
          |
          v
Bronze Delta tables
- bronze_hr_employees
- bronze_skill_extraction_logs
          |
          v
Silver standardized taxonomy
- silver_skill_taxonomy
- silver_employee_skills
          |
          v
Gold GraphFrames data product
- Employee and Skill vertices
- HAS_SKILL and REPORTS_TO edges
- gold_critical_skill_flight_risk
```

The design follows a Medallion pattern:

1. **Bronze** preserves source-system values for traceability.
2. **Silver** normalizes messy skill text against a governed taxonomy.
3. **Gold** creates a graph-based risk data product for executives, analytics, and AI consumption.

## Repository structure

```text
.
├── README.md
├── requirements.txt
├── ontology_schema.json
├── notebooks
│   ├── 01_bronze_ingestion.py
│   ├── 02_taxonomy_mapping.py
│   └── 03_graphframes_analytics.py
└── sample_output
    └── expected_results.md
```

## Data model

### Vertices

- **Employee**: employee identifier, name, organizational attributes, and engagement score.
- **Skill**: standardized skill identifier, category, criticality flag, and business criticality score.

### Edges

- **HAS_SKILL**: connects an employee to a standardized skill and carries proficiency evidence.
- **REPORTS_TO**: connects an employee to their manager.

Graph IDs use prefixes such as `EMP::E002` and `SKILL::SK001` to prevent collisions between entity types.

## How to run in Databricks

1. Create or use a Databricks cluster with PySpark and Delta Lake.
2. Install a GraphFrames library compatible with the cluster Spark and Scala runtime.
3. Import the files in `notebooks/` as Databricks notebooks or run them as Python files.
4. Execute them in order:
   - `01_bronze_ingestion.py`
   - `02_taxonomy_mapping.py`
   - `03_graphframes_analytics.py`
5. Review the Gold table:

```sql
SELECT *
FROM hive_metastore.talent_intelligence.gold_critical_skill_flight_risk
ORDER BY single_point_of_failure DESC,
         business_criticality DESC,
         engagement_score ASC;
```

## Local execution notes

The code is optimized for Databricks managed Delta tables. For local development, configure Spark with Delta Lake and install a GraphFrames package compatible with the local Spark runtime. GraphFrames package compatibility varies by Spark and Scala version, so the Databricks cluster library selector is the safest setup.

## Key business logic

An employee is included in the risk matrix when:

- the employee holds a governed critical skill;
- the latest engagement score is below `3.0`; and
- the graph contains a valid `HAS_SKILL` relationship.

The output also calculates the number of employees holding each skill:

- **CRITICAL**: one holder, creating a single point of failure;
- **HIGH**: two holders, creating limited resilience;
- **ELEVATED**: more than two holders, but at least one holder has low engagement.

## Enterprise extension path

This mini-project is intentionally small, but its architecture can scale into a production Talent Intelligence platform:

- Replace the hardcoded taxonomy with a versioned O*NET or enterprise skills taxonomy.
- Add effective dating and SCD Type 2 employee and organization dimensions.
- Separate slow-changing employee master data from high-frequency engagement and skill evidence.
- Add lineage and access controls through Unity Catalog.
- Publish the Gold risk table through a OneLake shortcut into Microsoft Fabric.
- Define executive metrics once in a Fabric semantic model for Power BI and AI agents.
- Add model outputs such as voluntary attrition probability as governed features, while retaining explainable business rules.

## Important limitation

The engagement threshold is an illustrative portfolio rule, not a production prediction model. Real employee risk use cases require legal, privacy, fairness, explainability, and human-review controls. The output should support workforce planning and retention conversations, not automated employment decisions.
