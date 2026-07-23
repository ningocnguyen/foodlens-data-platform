import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ---------------------------------------------------------
# Job parameters
# ---------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_database",
        "source_table",
        "gold_root",
        "processing_date",
        "run_id",
    ],
)

SOURCE_DATABASE = args["source_database"]
SOURCE_TABLE = args["source_table"]
GOLD_ROOT = args["gold_root"]
PROCESSING_DATE = args["processing_date"]
RUN_ID = args["run_id"]


# ---------------------------------------------------------
# Initialize Spark and Glue
# ---------------------------------------------------------

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


# ---------------------------------------------------------
# Read Silver from the Glue Data Catalog
# ---------------------------------------------------------

silver_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=SOURCE_DATABASE,
    table_name=SOURCE_TABLE,
    push_down_predicate=(
        f"processing_date='{PROCESSING_DATE}' "
        f"and run_id='{RUN_ID}'"
    ),
)

silver_df = silver_dynamic_frame.toDF()

source_count = silver_df.count()

print(f"Silver source count: {source_count}")


# ---------------------------------------------------------
# Normalize dimensions
# ---------------------------------------------------------

prepared_df = (
    silver_df
    .withColumn(
        "brand_clean",
        F.when(
            F.col("brand").isNull()
            | (F.trim(F.col("brand")) == ""),
            F.lit("Unknown"),
        ).otherwise(F.initcap(F.trim(F.col("brand")))),
    )
)


# ---------------------------------------------------------
# Build Gold brand summary
# ---------------------------------------------------------

brand_summary_df = (
    prepared_df
    .groupBy("brand_clean")
    .agg(
        F.count("*").alias("product_count"),
        F.round(
            F.avg("energy_kcal_100g"),
            2,
        ).alias("avg_energy_kcal_100g"),
        F.round(
            F.avg("proteins_100g"),
            2,
        ).alias("avg_proteins_100g"),
        F.round(
            F.avg("fat_100g"),
            2,
        ).alias("avg_fat_100g"),
        F.round(
            F.avg("sugars_100g"),
            2,
        ).alias("avg_sugars_100g"),
        F.round(
            F.avg("completeness_score"),
            4,
        ).alias("avg_completeness_score"),
    )
    .withColumnRenamed("brand_clean", "brand")
    .withColumn(
        "processing_date",
        F.lit(PROCESSING_DATE),
    )
    .withColumn(
        "run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "gold_generated_at",
        F.lit(datetime.now(timezone.utc)),
    )
)


# ---------------------------------------------------------
# Write Gold Parquet
# ---------------------------------------------------------

brand_summary_path = (
    f"{GOLD_ROOT}/brand_summary/"
    f"processing_date={PROCESSING_DATE}/"
    f"run_id={RUN_ID}"
)

(
    brand_summary_df
    .drop("processing_date", "run_id")
    .write
    .mode("overwrite")
    .parquet(brand_summary_path)
)


# ---------------------------------------------------------
# Operational logging
# ---------------------------------------------------------

brand_count = brand_summary_df.count()

print("Gold transformation completed")
print(f"Source Silver count: {source_count}")
print(f"Brand summary rows: {brand_count}")
print(f"Brand summary path: {brand_summary_path}")

brand_summary_df.orderBy(
    F.desc("product_count")
).show(
    20,
    truncate=False,
)

job.commit()