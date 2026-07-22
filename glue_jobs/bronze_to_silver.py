_import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


# Glue passes JOB_NAME automatically
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", "4")

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


RUN_ID = "20260721T044034Z"

BRONZE_PATH = (
    "s3://foodlens-ni-2026/bronze/"
    "ingestion_date=2026-07-21/"
    f"run_id={RUN_ID}/"
    "products.json"
)

SILVER_ROOT = "s3://foodlens-ni-2026/silver"
QUARANTINE_ROOT = "s3://foodlens-ni-2026/quarantine"


NUTRIMENTS_SCHEMA = StructType(
    [
        StructField("energy-kcal_100g", DoubleType(), True),
        StructField("fat_100g", DoubleType(), True),
        StructField("proteins_100g", DoubleType(), True),
        StructField("salt_100g", DoubleType(), True),
        StructField("sugars_100g", DoubleType(), True),
    ]
)

PRODUCT_SCHEMA = StructType(
    [
        StructField("_id", StringType(), True),
        StructField("code", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brands", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("countries", StringType(), True),
        StructField("ingredients_text", StringType(), True),
        StructField("allergens", StringType(), True),
        StructField("nutrition_grades", StringType(), True),
        StructField("last_modified_t", LongType(), True),
        StructField("nutriments", NUTRIMENTS_SCHEMA, True),
    ]
)


def missing_barcode_rule():
    return (
        F.col("barcode").isNull()
        | (F.trim(F.col("barcode")) == "")
    )


def missing_product_name_rule():
    return (
        F.col("product_name").isNull()
        | (F.trim(F.col("product_name")) == "")
    )


def invalid_nutrition_rule(column_name):
    return (
        F.col(column_name).isNotNull()
        & (
            (F.col(column_name) < 0)
            | (F.col(column_name) > 100)
        )
    )


def invalid_energy_rule():
    return (
        F.col("energy_kcal_100g").isNotNull()
        & (
            (F.col("energy_kcal_100g") < 0)
            | (F.col("energy_kcal_100g") > 1000)
        )
    )


def build_rejection_reason():
    return F.concat_ws(
        "; ",
        F.when(
            missing_barcode_rule(),
            F.lit("missing_barcode"),
        ),
        F.when(
            missing_product_name_rule(),
            F.lit("missing_product_name"),
        ),
        F.when(
            invalid_nutrition_rule("sugars_100g"),
            F.lit("invalid_sugars_100g"),
        ),
        F.when(
            invalid_nutrition_rule("fat_100g"),
            F.lit("invalid_fat_100g"),
        ),
        F.when(
            invalid_nutrition_rule("proteins_100g"),
            F.lit("invalid_proteins_100g"),
        ),
        F.when(
            invalid_nutrition_rule("salt_100g"),
            F.lit("invalid_salt_100g"),
        ),
        F.when(
            invalid_energy_rule(),
            F.lit("invalid_energy_kcal_100g"),
        ),
    )


print(f"Reading Bronze data from: {BRONZE_PATH}")

bronze_df = (
    spark.read
    .schema(PRODUCT_SCHEMA)
    .option("multiLine", True)
    .json(BRONZE_PATH)
)

source_count = bronze_df.count()

standardized_df = (
    bronze_df.select(
        F.coalesce(
            F.col("code"),
            F.col("_id"),
        ).alias("barcode"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.trim(F.col("brands")).alias("brand"),
        F.trim(F.col("categories")).alias("categories"),
        F.trim(F.col("countries")).alias("countries"),
        F.trim(F.col("ingredients_text")).alias(
            "ingredients_text"
        ),
        F.trim(F.col("allergens")).alias("allergens"),
        F.lower(
            F.trim(F.col("nutrition_grades"))
        ).alias("nutrition_grade"),
        F.col("last_modified_t")
        .cast("long")
        .alias("source_last_modified_timestamp"),
        F.col("nutriments.`energy-kcal_100g`")
        .cast("double")
        .alias("energy_kcal_100g"),
        F.col("nutriments.fat_100g")
        .cast("double")
        .alias("fat_100g"),
        F.col("nutriments.proteins_100g")
        .cast("double")
        .alias("proteins_100g"),
        F.col("nutriments.salt_100g")
        .cast("double")
        .alias("salt_100g"),
        F.col("nutriments.sugars_100g")
        .cast("double")
        .alias("sugars_100g"),
    )
    .withColumn(
        "source_last_modified_at",
        F.to_timestamp(
            F.from_unixtime(
                F.col("source_last_modified_timestamp")
            )
        ),
    )
    .withColumn(
        "pipeline_run_id",
        F.lit(RUN_ID),
    )
    .withColumn(
        "processed_at",
        F.current_timestamp(),
    )
)


important_columns = [
    "barcode",
    "product_name",
    "brand",
    "categories",
    "countries",
    "ingredients_text",
    "nutrition_grade",
    "energy_kcal_100g",
    "fat_100g",
    "proteins_100g",
    "salt_100g",
    "sugars_100g",
]

populated_expressions = [
    F.when(
        F.col(column_name).isNotNull()
        & (
            F.trim(
                F.col(column_name).cast("string")
            )
            != ""
        ),
        F.lit(1),
    ).otherwise(F.lit(0))
    for column_name in important_columns
]

populated_count = populated_expressions[0]

for expression in populated_expressions[1:]:
    populated_count = populated_count + expression

scored_df = standardized_df.withColumn(
    "completeness_score",
    F.round(
        populated_count / F.lit(len(important_columns)),
        4,
    ),
)

validated_df = scored_df.withColumn(
    "rejection_reason",
    build_rejection_reason(),
)

quarantine_df = validated_df.filter(
    F.length(F.col("rejection_reason")) > 0
)

valid_df = (
    validated_df.filter(
        F.length(F.col("rejection_reason")) == 0
    )
    .drop("rejection_reason")
)

ranking_window = (
    Window.partitionBy("barcode")
    .orderBy(
        F.col(
            "source_last_modified_timestamp"
        ).desc_nulls_last(),
        F.col("processed_at").desc(),
    )
)

deduplicated_df = (
    valid_df.withColumn(
        "row_number",
        F.row_number().over(ranking_window),
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

valid_count = deduplicated_df.count()
quarantined_count = quarantine_df.count()

processing_date = datetime.now(
    timezone.utc
).date().isoformat()

silver_path = (
    f"{SILVER_ROOT}"
    f"/processing_date={processing_date}"
    f"/run_id={RUN_ID}"
)

quarantine_path = (
    f"{QUARANTINE_ROOT}"
    f"/processing_date={processing_date}"
    f"/run_id={RUN_ID}"
)

print(f"Writing Silver data to: {silver_path}")
deduplicated_df.write.mode("overwrite").parquet(silver_path)

print(f"Writing Quarantine data to: {quarantine_path}")
quarantine_df.write.mode("overwrite").parquet(quarantine_path)

print("Transformation completed")
print(f"Source count: {source_count}")
print(f"Valid count after deduplication: {valid_count}")
print(f"Quarantined count: {quarantined_count}")
print(f"Silver path: {silver_path}")
print(f"Quarantine path: {quarantine_path}")

job.commit()