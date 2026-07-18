# FoodLens Data Platform

A production-style batch data platform that ingests food-product data from the Open Food Facts API, preserves recoverable raw data in Amazon S3, transforms nested JSON into curated Parquet datasets with PySpark on AWS Glue, enforces data-quality rules, and exposes analytics-ready Gold tables through the AWS Glue Data Catalog and Amazon Athena.

---

## Overview

FoodLens is an end-to-end data engineering project designed to demonstrate how a public API can be converted into a reliable, queryable cloud data platform.

The pipeline processes more than 2,000 food-product records per run and follows a Bronze, Silver, and Gold architecture:

- **Bronze:** recoverable raw API responses stored in Amazon S3
- **Silver:** validated, standardized, deduplicated product records in Parquet
- **Quarantine:** rejected records retained with traceable rejection reasons
- **Gold:** analytics-ready aggregate tables for brand, nutrition, and pipeline-quality reporting

The project separates local development, automated code validation, cloud storage, distributed processing, catalog registration, and SQL analytics.

---

## Architecture

```text
Open Food Facts API
        |
        v
Python Extraction Layer
        |
        v
Amazon S3 Bronze
Raw JSON + ingestion metadata
        |
        v
AWS Glue PySpark Job
Schema enforcement
Standardization
Validation
Deduplication
        |
        +----------------------+
        |                      |
        v                      v
Amazon S3 Silver         Amazon S3 Quarantine
Validated Parquet        Rejected Parquet
        |
        v
AWS Glue Gold Build
        |
        v
Amazon S3 Gold
Brand summary
Nutrition summary
Pipeline quality summary
        |
        v
AWS Glue Data Catalog
        |
        v
Amazon Athena
SQL analytics and validation
```

### Deployment and validation flow

```text
Developer Push
      |
      v
GitHub Actions
- Ruff linting
- Pytest unit tests
- Pytest integration tests
- Python syntax checks
      |
      v
Validated source code
      |
      v
Scheduled AWS Glue production run
```

---

## Key Features

- Ingests paginated food-product data from the Open Food Facts API
- Processes 2,000+ records per production run
- Uses retry handling and exponential backoff for temporary API failures
- Preserves raw JSON and extraction metadata in Amazon S3
- Applies explicit schemas to nested product and nutrition data
- Standardizes 12 product and nutrition fields
- Deduplicates products using barcode-based business keys
- Routes invalid rows to a quarantine layer
- Preserves traceable rejection reasons for auditability
- Writes partitioned Silver, Gold, and quarantine datasets in Parquet
- Runs transformations with PySpark on AWS Glue
- Registers curated datasets in the AWS Glue Data Catalog
- Exposes three Gold tables for Athena queries
- Produces a JSON pipeline report for every run
- Automates linting and tests with GitHub Actions
- Uses IAM roles instead of hardcoded AWS credentials
- Supports scheduled and repeatable cloud execution

---

## Tech Stack

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="PySpark">
  <img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="Amazon S3">
  <img src="https://img.shields.io/badge/AWS%20Glue-8C4FFF?style=for-the-badge&logo=amazonwebservices&logoColor=white" alt="AWS Glue">
  <img src="https://img.shields.io/badge/Amazon%20Athena-232F3E?style=for-the-badge&logo=amazonwebservices&logoColor=white" alt="Amazon Athena">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Parquet-50ABF1?style=for-the-badge&logo=apacheparquet&logoColor=white" alt="Apache Parquet">
  <img src="https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white" alt="GitHub Actions">
</p>

**Core stack:** Python, PySpark, Amazon S3, AWS Glue, Athena, Parquet, and GitHub Actions.

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Python, Open Food Facts API | Retrieve paginated source data with retries and metadata |
| Processing | PySpark on AWS Glue | Enforce schemas, standardize fields, validate, and deduplicate |
| Storage | Amazon S3, JSON, Parquet | Persist Bronze, Silver, Gold, quarantine, and report outputs |
| Catalog | AWS Glue Data Catalog | Store table definitions, schemas, and partitions |
| Analytics | Amazon Athena, SQL | Query curated Gold datasets directly from S3 |
| Orchestration | Amazon EventBridge, AWS Glue | Schedule and run repeatable production jobs |
| Monitoring | Amazon CloudWatch | Store job logs and support operational alerts |
| Security | AWS IAM, S3 encryption | Apply least-privilege access and protect stored data |
| Testing | Pytest | Validate quality rules and pipeline integration |
| CI/CD | GitHub Actions, Ruff | Run automated tests, linting, and deployment checks |

---

## Data Model

### Bronze layer

The Bronze layer stores the original API response with minimal modification.

```text
s3://<bucket-name>/bronze/
  ingestion_date=2026-07-18/
    run_id=20260718T195354Z/
      products.json
      metadata.json
```

Bronze data is retained so failed transformations can be replayed without calling the source API again.

### Silver layer

The Silver layer contains standardized, validated, and deduplicated product-level records.

```text
s3://<bucket-name>/silver/
  processing_date=2026-07-18/
    run_id=20260718T195354Z/
      part-*.snappy.parquet
```

Example Silver fields:

| Field | Description |
|---|---|
| barcode | Canonical product identifier |
| product_name | Standardized product name |
| brands | Brand text |
| categories | Product categories |
| countries | Countries where the product is sold |
| ingredients_text | Ingredient description |
| allergens | Declared allergens |
| nutrition_grade | Nutrition grade |
| energy_kcal_100g | Calories per 100 grams |
| fat_100g | Fat per 100 grams |
| sugars_100g | Sugar per 100 grams |
| proteins_100g | Protein per 100 grams |
| salt_100g | Salt per 100 grams |
| source_last_modified_at | Source modification timestamp |
| ingestion_timestamp | Pipeline ingestion timestamp |
| run_id | Pipeline run identifier |
| processing_date | Partition date |

### Quarantine layer

Rows that fail validation are retained rather than silently dropped.

```text
s3://<bucket-name>/quarantine/
  processing_date=2026-07-18/
    run_id=20260718T195354Z/
      part-*.snappy.parquet
```

Example rejection reasons:

```text
missing_barcode
missing_product_name
invalid_energy_kcal_100g
invalid_fat_100g
invalid_sugars_100g
invalid_proteins_100g
invalid_salt_100g
```

### Gold layer

The Gold layer contains analytics-ready aggregate datasets.

```text
s3://<bucket-name>/gold/
  processing_date=2026-07-18/
    run_id=20260718T195354Z/
      brand_summary/
      nutrition_grade_summary/
      pipeline_quality_summary/
```

#### Brand summary

- product count by brand
- average calories per 100 grams
- average sugar per 100 grams
- average fat per 100 grams
- percentage of records with complete nutrition data

#### Nutrition-grade summary

- product count by nutrition grade
- average calories
- average sugar
- average fat
- average protein
- average salt

#### Pipeline-quality summary

- extracted record count
- accepted record count
- quarantined record count
- acceptance rate
- quarantine rate
- rejection counts by reason
- duplicate count
- pipeline run timestamp

---

## Data-Quality Rules

| Rule | Action |
|---|---|
| Missing barcode | Quarantine |
| Missing product name | Quarantine |
| Duplicate barcode | Retain one canonical record |
| Negative nutrition value | Quarantine |
| Calories outside accepted range | Quarantine |
| Sugar outside accepted range | Quarantine |
| Fat outside accepted range | Quarantine |
| Protein outside accepted range | Quarantine |
| Salt outside accepted range | Quarantine |
| Invalid source timestamp | Standardize or quarantine based on rule |
| Missing optional descriptive fields | Preserve as null |
| Nested source fields | Flatten into typed Silver columns |

All rejected records remain available in S3 for auditing and future rule revisions.

---

## Example Production Run

Replace this section with values from a verified 2,000+ record deployment.

```json
{
  "run_id": "20260718T195354Z",
  "status": "success",
  "generated_at": "2026-07-18T20:15:20.457257+00:00",
  "source": "open_food_facts",
  "category": "chocolates",
  "extracted_record_count": 2000,
  "silver_record_count": 1894,
  "quarantined_record_count": 106,
  "acceptance_rate": 94.7,
  "brand_summary_count": 612,
  "nutrition_grade_summary_count": 5,
  "gold_table_count": 3,
  "quarantine_breakdown": {
    "missing_product_name": 71,
    "invalid_energy_kcal_100g": 23,
    "missing_barcode": 12
  }
}
```

> The values above are placeholders until a 2,000+ record AWS run has been completed and verified.

---

## Repository Structure

```text
foodlens-data-platform/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── deploy.yml
├── infrastructure/
│   ├── glue/
│   │   ├── create_tables.sql
│   │   └── crawler_config.json
│   └── iam/
│       └── glue_role_policy.json
├── src/
│   ├── api_client.py
│   ├── build_gold.py
│   ├── config.py
│   ├── extract.py
│   ├── quality.py
│   ├── reporting.py
│   ├── s3_publisher.py
│   ├── schemas.py
│   └── transform.py
├── tests/
│   ├── test_pipeline_integration.py
│   ├── test_quality.py
│   └── test_reporting.py
├── data/
│   └── samples/
│       └── products_sample.json
├── reports/
├── run_pipeline.py
├── requirements.txt
├── pytest.ini
├── .env.example
├── .gitignore
└── README.md
```

---

## Configuration

```bash
cp .env.example .env
```

Example `.env`:

```env
OPEN_FOOD_FACTS_BASE_URL=https://world.openfoodfacts.org
OPEN_FOOD_FACTS_CATEGORY=chocolates
OPEN_FOOD_FACTS_PAGE_SIZE=100
OPEN_FOOD_FACTS_MAX_PAGES=20
OPEN_FOOD_FACTS_USER_AGENT=FoodLensDataPlatform/1.0

BRONZE_ROOT=data/bronze
SILVER_ROOT=data/silver
GOLD_ROOT=data/gold
QUARANTINE_ROOT=data/quarantine
REPORT_ROOT=reports

AWS_REGION=us-east-1
S3_BUCKET_NAME=<your-private-bucket-name>
PUBLISH_TO_S3=true

GLUE_DATABASE_NAME=foodlens_gold
GLUE_JOB_NAME=foodlens-pyspark-job
ATHENA_OUTPUT_LOCATION=s3://<your-private-bucket-name>/athena-results/
```

Do not store AWS access keys in `.env`.

---

## Local Development

### Prerequisites

- Python 3.12
- Java 17
- Git
- AWS CLI
- AWS access to S3, Glue, IAM, and Athena

### Create the environment

```bash
conda create -n foodlens python=3.12 -y
conda activate foodlens
python -m pip install -r requirements.txt
```

Verify:

```bash
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

---

## Run the Pipeline Locally

```bash
python run_pipeline.py
```

Expected stages:

```text
Stage 1/4: Extract Bronze data
Stage 2/4: Build Silver and quarantine datasets
Stage 3/4: Build Gold datasets
Stage 4/4: Generate pipeline report
```

With S3 publishing enabled:

```text
Stage 5/5: Publish current run to Amazon S3
```

Inspect the newest report:

```bash
latest_report=$(
  find reports -name pipeline_report.json -type f -print0 |
  xargs -0 ls -t |
  head -n 1
)

cat "$latest_report"
```

---

## Testing

```bash
python -m pytest -v
python -m pytest tests/test_pipeline_integration.py -v
ruff check src tests run_pipeline.py
python -m py_compile src/*.py run_pipeline.py
```

### Test strategy

Unit tests validate individual rules, including:

- missing barcode detection
- missing product-name detection
- invalid nutrition ranges
- rejection-reason composition
- report serialization

The integration test processes a fixed Bronze sample through the real transformation logic and verifies Silver and quarantine outputs.

GitHub Actions runs linting, unit tests, integration tests, and syntax checks on pushes and pull requests.

---

## Amazon S3 Layout

```text
s3://<bucket-name>/
├── bronze/
│   └── ingestion_date=YYYY-MM-DD/
│       └── run_id=<run-id>/
├── silver/
│   └── processing_date=YYYY-MM-DD/
│       └── run_id=<run-id>/
├── quarantine/
│   └── processing_date=YYYY-MM-DD/
│       └── run_id=<run-id>/
├── gold/
│   └── processing_date=YYYY-MM-DD/
│       └── run_id=<run-id>/
├── reports/
│   └── run_id=<run-id>/
└── athena-results/
```

The bucket remains private with public access blocked and default encryption enabled.

---

## AWS Glue Deployment

The Glue job:

1. Reads Bronze JSON from S3
2. Applies the explicit product schema
3. Standardizes nested fields
4. Validates required and numeric fields
5. Deduplicates records by barcode
6. Writes valid records to Silver
7. Writes rejected records to quarantine
8. Builds Gold aggregate tables
9. Writes a pipeline report
10. Updates catalog partitions

Recommended configuration:

```text
Job type: Spark
Language: Python
Worker type: G.1X or smallest suitable worker
IAM role: Dedicated least-privilege Glue role
Timeout: Configured to prevent runaway costs
Retries: 1 or 2
```

The Glue role should only read and write required S3 prefixes, update the Data Catalog, and write CloudWatch logs.

---

## AWS Glue Data Catalog

Database:

```text
foodlens_gold
```

Tables:

```text
brand_summary
nutrition_grade_summary
pipeline_quality_summary
```

Recommended stable table locations:

```text
s3://<bucket-name>/gold/brand_summary/
s3://<bucket-name>/gold/nutrition_grade_summary/
s3://<bucket-name>/gold/pipeline_quality_summary/
```

Recommended partitions:

```text
processing_date
run_id
```

---

## Athena Queries

Configure query results:

```text
s3://<bucket-name>/athena-results/
```

### Top brands

```sql
SELECT
    brand,
    product_count,
    average_sugars_100g,
    average_energy_kcal_100g
FROM foodlens_gold.brand_summary
ORDER BY product_count DESC
LIMIT 20;
```

### Compare nutrition grades

```sql
SELECT
    nutrition_grade,
    product_count,
    average_energy_kcal_100g,
    average_sugars_100g,
    average_fat_100g
FROM foodlens_gold.nutrition_grade_summary
ORDER BY nutrition_grade;
```

### Review pipeline quality

```sql
SELECT
    processing_date,
    run_id,
    extracted_record_count,
    silver_record_count,
    quarantined_record_count,
    acceptance_rate
FROM foodlens_gold.pipeline_quality_summary
ORDER BY processing_date DESC, run_id DESC
LIMIT 20;
```

---

## Scheduling

The production pipeline runs on a schedule through an AWS-native scheduler connected to the Glue job.

Example:

```text
Daily at 06:00 UTC
```

Each run produces:

- one Bronze run directory
- one Silver run directory
- one quarantine run directory
- three Gold datasets
- one JSON report
- CloudWatch logs
- updated catalog partitions

---

## GitHub Actions

The CI workflow validates:

- dependency installation
- Java availability
- Ruff
- Pytest
- integration tests
- syntax compilation

A separate deployment workflow can package Glue scripts, upload versioned code to S3, and update the Glue job definition. Production ETL scheduling remains separate from normal CI.

---

## Security

- Keep S3 private
- Block all public access
- Enable default encryption
- Use IAM roles for Glue
- Apply least-privilege policies
- Never commit credentials
- Store deployment secrets in GitHub Actions secrets
- Enable CloudWatch logs
- Configure AWS Budgets
- Configure Athena workgroup limits

---

## Cost Controls

- Create a monthly AWS Budget alert
- Use the smallest suitable Glue worker configuration
- Set Glue job timeouts
- Write Parquet instead of repeatedly querying JSON
- Partition datasets to reduce Athena scan volume
- Add S3 lifecycle rules for temporary files
- Delete unused test resources
- Use Athena workgroups with scan limits

---

## Observability

Each run reports:

- run ID
- status
- source
- category
- generated timestamp
- S3 paths
- extracted count
- Silver count
- quarantined count
- Gold row counts
- rejection breakdown
- acceptance rate
- execution duration
- Glue job-run identifier

CloudWatch logs capture API retries, stage transitions, record counts, output paths, and failures.

---

## Failure Handling

### API failure

Temporary failures are retried with increasing wait times. If extraction still fails, the run exits without publishing incomplete curated outputs.

### Transformation failure

Bronze data remains in S3 and can be replayed without another API request.

### Invalid records

Invalid records are written to quarantine with rejection reasons and excluded from Gold metrics.

### Glue failure

CloudWatch retains job logs, and earlier successful outputs remain unchanged.

---

## Performance and Scaling

The completed deployment targets 2,000+ records per run.

Further scaling options:

- increase page count
- use a bulk source dataset
- increase Glue workers
- tune Spark partitions
- compact small Parquet files
- add incremental loading
- orchestrate stages with Step Functions
- schedule through EventBridge
- add CloudWatch alerts

Measure before publishing performance claims:

- total run duration
- stage durations
- output sizes
- Athena scanned bytes
- Glue worker configuration
- accepted and quarantined counts

---

## Verified Completion Checklist

### Data volume

- [ ] One successful run processed at least 2,000 records
- [ ] Counts were verified in the report
- [ ] Run ID and timestamp were recorded

### S3

- [ ] Bronze JSON stored in S3
- [ ] Silver Parquet stored in S3
- [ ] Quarantine Parquet stored in S3
- [ ] Gold Parquet stored in S3
- [ ] Reports stored in S3
- [ ] Public access blocked
- [ ] Encryption enabled

### AWS Glue

- [ ] PySpark transformation runs on Glue
- [ ] Glue job uses IAM role
- [ ] Logs visible in CloudWatch
- [ ] Scheduled run completes
- [ ] Job does not depend on local paths

### Catalog and Athena

- [ ] Glue database exists
- [ ] Three Gold tables registered
- [ ] Athena queries all three tables
- [ ] Query output stored in private S3
- [ ] Example queries return expected results

### Quality

- [ ] Schema enforcement active
- [ ] Barcode deduplication verified
- [ ] Invalid records quarantined
- [ ] Rejection reasons preserved
- [ ] Breakdown included in reports

### Engineering workflow

- [ ] Unit tests pass
- [ ] Integration test passes
- [ ] Ruff passes
- [ ] GitHub Actions is green
- [ ] Credentials are not committed
- [ ] README metrics match verified output

---

## Example Resume Description

**FoodLens Data Platform**  
*Python, PySpark, Amazon S3, AWS Glue, Athena, Parquet, GitHub Actions*

- Built a deployable AWS batch data pipeline processing 2,000+ food-product records per run, ingesting nested JSON into Amazon S3 and transforming it into partitioned Silver and Gold Parquet datasets with PySpark on AWS Glue.
- Enforced explicit schemas and quality rules across 12 product and nutrition fields, deduplicated barcodes, and routed invalid records to a traceable S3 quarantine layer.
- Registered three curated Gold datasets in the AWS Glue Data Catalog and exposed them through Athena for brand, nutrition, and pipeline-quality analysis.
- Automated linting, unit tests, integration tests, and deployment validation with Ruff, Pytest, and GitHub Actions while IAM roles and scheduled Glue jobs supported secure, repeatable production runs.

Use these bullets only after all claims are verified.

---

## Future Enhancements

- Incremental loading
- AWS Step Functions orchestration
- Amazon EventBridge scheduling
- CloudWatch alarms
- Great Expectations or Deequ
- Terraform or AWS CDK
- Athena views
- QuickSight dashboard
- S3 lifecycle policies
- Data lineage documentation
- Schema-evolution handling
- Multiple food categories
- Bulk dataset ingestion

---

## License

This project is intended for educational and portfolio use.

Open Food Facts data is provided by Open Food Facts and is subject to its own licensing and attribution requirements.

---

## Author

**Ni Nguyen**

GitHub: [ningocnguyen](https://github.com/ningocnguyen)
