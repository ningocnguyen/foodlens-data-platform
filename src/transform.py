"""Transform Bronze Open Food Facts data into Silver datasets"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import Settings
from src.quality import build_rejection_reason
from src.schemas import PRODUCT_SCHEMA


@dataclass(frozen=True)
class TransformationResult:
    """Summary of one Bronze-to-Silver transformation"""

    run_id: str
    silver_path: Path
    quarantine_path: Path
    source_count: int
    valid_count: int
    quarantined_count: int


def create_spark_session() -> SparkSession:
    """Create the local Spark session used by the pipeline"""

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("FoodLensDataPlatform")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_bronze_products(
    spark: SparkSession,
    bronze_product_path: Path,
) -> DataFrame:
    """Read Bronze JSON using the explicit product schema"""

    return (
        spark.read
        .schema(PRODUCT_SCHEMA)
        .option("multiLine", True)
        .json(str(bronze_product_path))
    )


def standardize_products(
    bronze_df: DataFrame,
    run_id: str,
) -> DataFrame:
    """Select, rename, cast, and enrich useful product fields"""

    return (
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
            F.col("last_modified_t").cast("long").alias(
                "source_last_modified_timestamp"
            ),
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
            F.lit(run_id),
        )
        .withColumn(
            "processed_at",
            F.current_timestamp(),
        )
    )


def add_completeness_score(
    products_df: DataFrame,
) -> DataFrame:
    """Calculate the percentage of important fields that are populated"""

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

    return products_df.withColumn(
        "completeness_score",
        F.round(
            populated_count / F.lit(len(important_columns)),
            4,
        ),
    )


def deduplicate_products(
    valid_df: DataFrame,
) -> DataFrame:
    """Keep the most recently modified row for each barcode"""

    ranking_window = (
        Window.partitionBy("barcode")
        .orderBy(
            F.col(
                "source_last_modified_timestamp"
            ).desc_nulls_last(),
            F.col("processed_at").desc(),
        )
    )

    return (
        valid_df.withColumn(
            "row_number",
            F.row_number().over(ranking_window),
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )


def transform_products(
    spark: SparkSession,
    settings: Settings,
    bronze_product_path: Path,
    run_id: str,
) -> TransformationResult:
    """Transform raw Bronze products into Silver and Quarantine"""

    bronze_df = read_bronze_products(
        spark=spark,
        bronze_product_path=bronze_product_path,
    )

    source_count = bronze_df.count()

    standardized_df = standardize_products(
        bronze_df=bronze_df,
        run_id=run_id,
    )

    scored_df = add_completeness_score(standardized_df)

    validated_df = scored_df.withColumn(
        "rejection_reason",
        build_rejection_reason(),
    )

    quarantine_df = validated_df.filter(
        F.length(F.col("rejection_reason")) > 0
    )

    valid_df = validated_df.filter(
        F.length(F.col("rejection_reason")) == 0
    ).drop("rejection_reason")

    deduplicated_df = deduplicate_products(valid_df)

    valid_count = deduplicated_df.count()
    quarantined_count = quarantine_df.count()

    processing_date = datetime.now(
        timezone.utc
    ).date().isoformat()

    silver_path = (
        Path(settings.silver_root)
        / f"processing_date={processing_date}"
        / f"run_id={run_id}"
    )

    quarantine_path = (
        Path(settings.quarantine_root)
        / f"processing_date={processing_date}"
        / f"run_id={run_id}"
    )

    deduplicated_df.write.mode("overwrite").parquet(
        str(silver_path)
    )

    quarantine_df.write.mode("overwrite").parquet(
        str(quarantine_path)
    )

    return TransformationResult(
        run_id=run_id,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
        source_count=source_count,
        valid_count=valid_count,
        quarantined_count=quarantined_count,
    )