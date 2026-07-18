# FoodLens Data Platform

FoodLens is a batch data pipeline that collects food-product data from the Open Food Facts API, cleans and validates it with PySpark, stores each stage in Amazon S3, and makes the final tables available for SQL queries in Amazon Athena.

The project focuses on the work between receiving raw data and delivering reliable tables for analysis.

---

## What the pipeline does

1. Pulls product data from the Open Food Facts API
2. Saves the original response in an S3 Bronze layer
3. Uses an AWS Glue PySpark job to clean and validate the data
4. Sends valid records to Silver and invalid records to quarantine
5. Builds Gold summary tables
6. Registers the Gold tables in the AWS Glue Data Catalog
7. Queries the final tables with Amazon Athena
8. Runs tests and code checks through GitHub Actions

---

## Architecture

```text
Open Food Facts API
        |
        v
Python extraction
        |
        v
Amazon S3 Bronze
Raw JSON and run metadata
        |
        v
AWS Glue PySpark job
Clean, validate, and deduplicate
        |
        +--------------------+
        |                    |
        v                    v
Amazon S3 Silver      Amazon S3 Quarantine
Valid Parquet data    Rejected records
        |
        v
Amazon S3 Gold
Summary tables
        |
        v
AWS Glue Data Catalog
        |
        v
Amazon Athena
SQL queries
```

---

## Tech Stack

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="PySpark">
  <img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="Amazon S3">
  <img src="https://img.shields.io/badge/AWS%20Glue-8C4FFF?style=for-the-badge&logo=amazonwebservices&logoColor=white" alt="AWS Glue">
  <img src="https://img.shields.io/badge/Amazon%20Athena-232F3E?style=for-the-badge&logo=amazonwebservices&logoColor=white" alt="Amazon Athena">
  <img src="https://img.shields.io/badge/Parquet-50ABF1?style=for-the-badge&logo=apacheparquet&logoColor=white" alt="Parquet">
  <img src="https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white" alt="GitHub Actions">
</p>

---

## Data layers

### Bronze

Bronze stores the original API response and basic run metadata.

```text
s3://<bucket-name>/bronze/
  ingestion_date=YYYY-MM-DD/
    run_id=<run-id>/
      products.json
      metadata.json
```

Keeping the raw response makes it possible to rerun the transformation without calling the API again.

### Silver

Silver contains product records that passed validation.

```text
s3://<bucket-name>/silver/
  processing_date=YYYY-MM-DD/
    run_id=<run-id>/
      part-*.snappy.parquet
```

Main fields include barcode, product name, brand, category, country, ingredients, allergens, nutrition grade, nutrition values, timestamps, and run ID.

### Quarantine

Records that fail validation are kept instead of being deleted.

```text
s3://<bucket-name>/quarantine/
  processing_date=YYYY-MM-DD/
    run_id=<run-id>/
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

### Gold

Gold contains tables ready for reporting and Athena queries.

```text
s3://<bucket-name>/gold/
  processing_date=YYYY-MM-DD/
    run_id=<run-id>/
      brand_summary/
      nutrition_grade_summary/
      pipeline_quality_summary/
```

The three Gold tables are:

- **Brand summary:** product counts and average nutrition values by brand
- **Nutrition-grade summary:** product counts and nutrition averages by grade
- **Pipeline-quality summary:** input, accepted, rejected, duplicate, and rejection counts by run

---

## Data-quality rules

| Check | Result |
|---|---|
| Missing barcode | Send to quarantine |
| Missing product name | Send to quarantine |
| Duplicate barcode | Keep one record using a documented rule |
| Negative nutrition value | Send to quarantine |
| Nutrition value outside the accepted range | Send to quarantine |
| Missing optional field | Keep the record with a null value |

Rejected records remain available for review.

---

## Example run report

Each pipeline run creates a JSON report.

```json
{
  "run_id": "20260718T195354Z",
  "status": "success",
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

Replace these example values with results from a verified run.

---

## Repository structure

```text
foodlens-data-platform/
├── .github/workflows/
├── src/
├── tests/
├── data/samples/
├── run_pipeline.py
├── requirements.txt
├── pytest.ini
├── .env.example
└── README.md
```

---

## Configuration

```bash
cp .env.example .env
```

Example:

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

Do not store AWS access keys in this file.

---

## Run locally

```bash
conda create -n foodlens python=3.12 -y
conda activate foodlens
python -m pip install -r requirements.txt
python run_pipeline.py
```

---

## Testing

```bash
python -m pytest -v
python -m pytest tests/test_pipeline_integration.py -v
ruff check src tests run_pipeline.py
```

GitHub Actions runs these checks automatically on pushes and pull requests.

---

## AWS setup

- **Amazon S3:** stores Bronze, Silver, quarantine, Gold, reports, and Athena results
- **AWS Glue:** runs the PySpark transformation
- **Glue Data Catalog:** stores table definitions
- **Amazon Athena:** queries the Gold tables
- **CloudWatch:** stores job logs
- **IAM:** gives the Glue job access to only the resources it needs

---

## Example Athena query

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

---

## Scheduling and monitoring

The Glue job runs on a schedule. Each run creates new data, a JSON report, and CloudWatch logs.

GitHub Actions is used for tests and deployment checks. AWS is used for scheduled data runs.

---

## Main engineering lessons

- Save raw data before transforming it so failed jobs can be replayed
- Keep invalid records with a reason instead of deleting them
- Use explicit schemas for inconsistent API data
- Use JSON for raw data and Parquet for cleaned analytical data
- Separate code testing from scheduled production runs
- Treat retries as protection from temporary failures, not as a scaling strategy
- Choose partitions based on how the data will be queried
- Track record counts and rejection reasons for every run

---

## Future improvements

- Incremental loading
- Better schema-change handling
- Infrastructure as code
- CloudWatch alerts
- S3 lifecycle policies
- Bulk Open Food Facts ingestion
- More food categories
- Dashboard or Athena views
