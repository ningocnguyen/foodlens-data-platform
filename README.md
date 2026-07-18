# FoodLens Data Platform

![FoodLens CI](https://github.com/ningocnguyen/foodlens-data-platform/actions/workflows/ci.yml/badge.svg)
![Daily Pipeline](https://github.com/ningocnguyen/foodlens-data-platform/actions/workflows/daily_pipeline.yml/badge.svg)

FoodLens is an automated batch data pipeline that converts raw Open Food Facts API responses into validated, analytics-ready datasets for product, nutrition, and data-quality analysis.

The project demonstrates how external API data can be preserved, standardized, validated, aggregated, tested, and executed on a schedule.

## Business Problem

Open Food Facts provides useful product and nutrition information, but the raw API response is not directly suitable for reliable analysis.

Common issues include:

- nested JSON structures;
- missing product names or barcodes;
- duplicate product records;
- incomplete brand and nutrition information;
- nutrition values outside reasonable ranges;
- inconsistent field formats.

Without a standardized pipeline, analysts would need to repeat the same cleaning and validation logic for every report.

FoodLens creates reusable datasets that support questions such as:

- How many products are represented by each brand?
- How do sugar, fat, salt, and calorie values vary by nutrition grade?
- What percentage of each ingestion run passes validation?
- Which records were rejected, and why?
- How complete is the source data?

## Architecture

```text
Open Food Facts API
        |
        v
Bronze Layer
Raw JSON + ingestion metadata
        |
        v
Silver Layer
Schema enforcement, cleaning, validation, deduplication
        |
        +----------------------+
        |                      |
        v                      v
Valid Parquet Data       Quarantine Parquet
                         Rejection reasons
        |
        v
Gold Layer
Brand summary
Nutrition-grade summary
Pipeline-quality summary
        |
        v
JSON Pipeline Report
GitHub Actions Artifact
```

## Data Layers

### Bronze

The Bronze layer preserves the API response with minimal modification.

Outputs include:

- raw product records in JSON;
- extraction timestamp;
- source category;
- page and record counts;
- unique pipeline run ID.

Example path:

```text
data/bronze/
└── ingestion_date=YYYY-MM-DD/
    └── run_id=YYYYMMDDTHHMMSSZ/
        ├── products.json
        └── metadata.json
```

Preserving the raw source makes it possible to reprocess data without calling the API again.

### Silver

The Silver layer converts raw records into standardized product-level data.

Processing includes:

- explicit PySpark schema enforcement;
- column selection and renaming;
- data-type conversion;
- text normalization;
- barcode-based deduplication;
- product completeness scoring;
- nutrition-range validation;
- separation of valid and invalid records.

Valid records are written to:

```text
data/silver/
```

Rejected records are written to:

```text
data/quarantine/
```

Each quarantined record includes a reason such as:

```text
missing_barcode
missing_product_name
invalid_sugars_100g
invalid_fat_100g
invalid_energy_kcal_100g
```

### Gold

The Gold layer contains prepared datasets for reporting and analysis.

FoodLens currently creates three Gold tables.

#### Brand Summary

Provides:

- product count by brand;
- average completeness score;
- average sugar per 100g;
- average fat per 100g;
- average calories per 100g.

#### Nutrition Grade Summary

Provides:

- product count by nutrition grade;
- average sugar;
- average fat;
- average salt;
- average calories;
- average completeness score.

#### Pipeline Quality Summary

Provides:

- source record count;
- valid record count;
- quarantined record count;
- acceptance rate;
- quarantine rate;
- average completeness score;
- minimum and maximum completeness scores;
- low-completeness record count.

## Technology Decisions

| Tool             | Purpose                                                     | Reason for Selection                                                     |
| ---------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------ |
| Python           | API extraction, configuration, orchestration, and reporting | Connects API requests, Spark processing, and pipeline control            |
| PySpark          | Validation, transformation, deduplication, and aggregation  | Supports explicit schemas and scalable transformation logic              |
| JSON             | Bronze storage                                              | Preserves the original nested API response for recovery and reprocessing |
| Parquet          | Silver, Gold, and quarantine storage                        | Supports efficient analytical reads without repeatedly scanning raw JSON |
| Pytest           | Unit testing                                                | Verifies that data-quality rules behave correctly                        |
| Ruff             | Static code checks                                          | Detects common Python issues and keeps code consistent                   |
| GitHub Actions   | CI and scheduled execution                                  | Runs tests and pipelines in a clean remote Linux environment             |
| GitHub Artifacts | Pipeline-output retention                                   | Preserves generated datasets without committing them to Git history      |

## Why PySpark?

The current dataset can fit in memory, so pandas would be simpler for a small production workload.

PySpark was selected to implement:

- explicit schema enforcement;
- reusable column transformations;
- window-based barcode deduplication;
- partitioned Parquet outputs;
- transformation patterns that can support larger batches.

This project treats Spark as a design and scaling decision, not a requirement for the current data volume.

## Project Structure

```text
foodlens-data-platform/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── daily_pipeline.yml
├── dashboard/
│   └── app.py
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── quarantine/
│   └── samples/
├── docs/
│   ├── architecture.md
│   └── data_dictionary.md
├── reports/
├── src/
│   ├── api_client.py
│   ├── build_gold.py
│   ├── config.py
│   ├── extract.py
│   ├── quality.py
│   ├── reporting.py
│   ├── schemas.py
│   └── transform.py
├── tests/
│   └── test_quality.py
├── .env.example
├── .gitignore
├── pytest.ini
├── requirements.txt
└── run_pipeline.py
```

## Pipeline Execution

The entire pipeline can be run with:

```bash
python run_pipeline.py
```

The command performs:

1. API extraction;
2. Bronze JSON creation;
3. Silver transformation;
4. quarantine processing;
5. Gold aggregation;
6. JSON report generation.

A successful run prints:

```text
FoodLens pipeline completed successfully
Run ID: 20260714T...
Extracted records: 10
Silver records: 10
Quarantined records: 0
Gold path: data/gold/...
Report path: reports/run_id=.../pipeline_report.json
```

## Local Setup

### 1. Clone the repository

```bash
git clone https://github.com/ningocnguyen/foodlens-data-platform.git
cd foodlens-data-platform
```

### 2. Create and activate the environment

```bash
conda create -n foodlens python=3.12 -y
conda activate foodlens
```

### 3. Install dependencies

```bash
python -m pip install -r requirements.txt
```

### 4. Create local configuration

```bash
cp .env.example .env
```

Update the user-agent value in `.env`:

```env
FOODLENS_USER_AGENT="FoodLensDataPlatform/1.0 (your-email@example.com)"
```

### 5. Run tests

```bash
python -m pytest -v
```

### 6. Run code checks

```bash
ruff check src tests run_pipeline.py
```

### 7. Run the pipeline

```bash
python run_pipeline.py
```

## Data Quality Rules

A record is quarantined when it contains one or more of the following:

| Rule                 | Description                                      |
| -------------------- | ------------------------------------------------ |
| Missing barcode      | Product cannot be uniquely identified            |
| Missing product name | Record is not useful for product-level reporting |
| Invalid sugar        | Value is below 0 or above 100g per 100g          |
| Invalid fat          | Value is below 0 or above 100g per 100g          |
| Invalid protein      | Value is below 0 or above 100g per 100g          |
| Invalid salt         | Value is below 0 or above 100g per 100g          |
| Invalid calories     | Value is below 0 or above 1,000 kcal per 100g    |

Rejected records are retained instead of deleted so source-data issues remain traceable.

## Automated Testing

The CI workflow runs on pushes and pull requests to `main`.

It performs:

```text
Dependency installation
Java and Python setup
Ruff checks
Pytest unit tests
```

Workflow:

```text
.github/workflows/ci.yml
```

## Scheduled Pipeline

The scheduled workflow runs the complete pipeline in GitHub Actions.

It:

- installs Python and Java;
- executes `run_pipeline.py`;
- creates Bronze, Silver, Gold, quarantine, and report outputs;
- uploads generated files as a GitHub Actions artifact;
- retains artifacts for 30 days.

Workflow:

```text
.github/workflows/daily_pipeline.yml
```

## Current Results

| Metric                   |  Result |
| ------------------------ | ------: |
| Source records processed |      10 |
| Fields standardized      |      12 |
| Gold tables produced     |       3 |
| Automated quality tests  |       4 |
| Output groups retained   |       5 |
| Artifact retention       | 30 days |

## Future Improvements

- store Bronze, Silver, Gold, and quarantine datasets in Amazon S3;
- execute PySpark transformations through AWS Glue;
- register Gold schemas in the AWS Glue Data Catalog;
- query Gold tables through Amazon Athena;
- add an end-to-end integration test using a fixed sample dataset;
- add per-stage timing and duplicate-count metrics;
- create a lightweight dashboard from Gold datasets.

## Key Engineering Decisions

- Raw source data is preserved so transformations can be rerun.
- Invalid records are quarantined rather than silently dropped.
- Gold metrics are precomputed so downstream users do not repeat transformation logic.
- Generated data is stored as workflow artifacts rather than committed to Git.
- CI and pipeline execution are separate workflows because they serve different purposes.
