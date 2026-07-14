"""Build analytics-ready Gold datasets from Silver products"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import Settings


@dataclass(frozen=True)
class GoldBuildResult:
    """Summary of datasets created during the Gold build"""

    run_id: str
    gold_run_path: Path
    brand_summary_path: Path
    nutrition_grade_summary_path: Path
    pipeline_quality_summary_path: Path
    brand_summary_count: int
    nutrition_grade_summary_count: int


def build_gold_datasets(
    spark: SparkSession,
    settings: Settings,
    silver_path: Path,
    run_id: str,
    source_count: int,
    quarantined_count: int,
) -> GoldBuildResult:
    """Create analytics summaries from a Silver dataset"""

    silver_df = spark.read.parquet(str(silver_path))

    valid_count = silver_df.count()

    processing_date = datetime.now(
        timezone.utc
    ).date().isoformat()

    gold_run_path = (
        Path(settings.gold_root)
        / f"processing_date={processing_date}"
        / f"run_id={run_id}"
    )

    brand_summary_path = gold_run_path / "brand_summary"

    nutrition_grade_summary_path = (
        gold_run_path / "nutrition_grade_summary"
    )

    pipeline_quality_summary_path = (
        gold_run_path / "pipeline_quality_summary"
    )

    brand_summary_df = (
        silver_df
        .withColumn(
            "brand_group",
            F.when(
                F.col("brand").isNull()
                | (F.trim(F.col("brand")) == ""),
                F.lit("Unknown"),
            ).otherwise(F.col("brand")),
        )
        .groupBy("brand_group")
        .agg(
            F.countDistinct("barcode").alias(
                "product_count"
            ),
            F.round(
                F.avg("completeness_score"),
                4,
            ).alias("average_completeness_score"),
            F.round(
                F.avg("sugars_100g"),
                2,
            ).alias("average_sugars_100g"),
            F.round(
                F.avg("fat_100g"),
                2,
            ).alias("average_fat_100g"),
            F.round(
                F.avg("energy_kcal_100g"),
                2,
            ).alias("average_energy_kcal_100g"),
        )
        .withColumnRenamed(
            "brand_group",
            "brand",
        )
        .orderBy(
            F.col("product_count").desc(),
            F.col("brand").asc(),
        )
    )

    nutrition_grade_summary_df = (
        silver_df
        .withColumn(
            "grade_group",
            F.when(
                F.col("nutrition_grade").isNull()
                | (
                    F.trim(
                        F.col("nutrition_grade")
                    )
                    == ""
                ),
                F.lit("unknown"),
            ).otherwise(
                F.lower(F.col("nutrition_grade"))
            ),
        )
        .groupBy("grade_group")
        .agg(
            F.countDistinct("barcode").alias(
                "product_count"
            ),
            F.round(
                F.avg("sugars_100g"),
                2,
            ).alias("average_sugars_100g"),
            F.round(
                F.avg("fat_100g"),
                2,
            ).alias("average_fat_100g"),
            F.round(
                F.avg("salt_100g"),
                3,
            ).alias("average_salt_100g"),
            F.round(
                F.avg("energy_kcal_100g"),
                2,
            ).alias("average_energy_kcal_100g"),
            F.round(
                F.avg("completeness_score"),
                4,
            ).alias("average_completeness_score"),
        )
        .withColumnRenamed(
            "grade_group",
            "nutrition_grade",
        )
        .orderBy("nutrition_grade")
    )

    acceptance_rate = (
        valid_count / source_count
        if source_count > 0
        else 0.0
    )

    quarantine_rate = (
        quarantined_count / source_count
        if source_count > 0
        else 0.0
    )

    pipeline_quality_summary_df = (
        silver_df
        .agg(
            F.round(
                F.avg("completeness_score"),
                4,
            ).alias("average_completeness_score"),
            F.min("completeness_score").alias(
                "minimum_completeness_score"
            ),
            F.max("completeness_score").alias(
                "maximum_completeness_score"
            ),
            F.sum(
                F.when(
                    F.col("completeness_score") < 0.5,
                    1,
                ).otherwise(0)
            ).alias("low_completeness_count"),
        )
        .withColumn(
            "run_id",
            F.lit(run_id),
        )
        .withColumn(
            "source_record_count",
            F.lit(source_count),
        )
        .withColumn(
            "valid_record_count",
            F.lit(valid_count),
        )
        .withColumn(
            "quarantined_record_count",
            F.lit(quarantined_count),
        )
        .withColumn(
            "acceptance_rate",
            F.lit(round(acceptance_rate, 4)),
        )
        .withColumn(
            "quarantine_rate",
            F.lit(round(quarantine_rate, 4)),
        )
        .withColumn(
            "generated_at",
            F.current_timestamp(),
        )
        .select(
            "run_id",
            "source_record_count",
            "valid_record_count",
            "quarantined_record_count",
            "acceptance_rate",
            "quarantine_rate",
            "average_completeness_score",
            "minimum_completeness_score",
            "maximum_completeness_score",
            "low_completeness_count",
            "generated_at",
        )
    )

    brand_summary_df.write.mode(
        "overwrite"
    ).parquet(
        str(brand_summary_path)
    )

    nutrition_grade_summary_df.write.mode(
        "overwrite"
    ).parquet(
        str(nutrition_grade_summary_path)
    )

    pipeline_quality_summary_df.write.mode(
        "overwrite"
    ).parquet(
        str(pipeline_quality_summary_path)
    )

    return GoldBuildResult(
        run_id=run_id,
        gold_run_path=gold_run_path,
        brand_summary_path=brand_summary_path,
        nutrition_grade_summary_path=(
            nutrition_grade_summary_path
        ),
        pipeline_quality_summary_path=(
            pipeline_quality_summary_path
        ),
        brand_summary_count=brand_summary_df.count(),
        nutrition_grade_summary_count=(
            nutrition_grade_summary_df.count()
        ),
    )