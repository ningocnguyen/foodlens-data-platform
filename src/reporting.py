"""Generate JSON reports for completed FoodLens pipeline runs"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

from pyspark.sql import SparkSession

from src.build_gold import GoldBuildResult
from src.extract import ExtractionResult
from src.transform import TransformationResult


@dataclass(frozen=True)
class PipelineReport:
    """Serializable summary of one pipeline execution"""

    run_id: str
    status: str
    generated_at: str
    source: str
    category: str
    bronze_product_path: str
    silver_path: str
    quarantine_path: str
    gold_path: str
    extracted_record_count: int
    silver_record_count: int
    quarantined_record_count: int
    brand_summary_count: int
    nutrition_grade_summary_count: int
    quarantine_breakdown: dict[str, int]


def get_quarantine_breakdown(
    spark: SparkSession,
    quarantine_path: Path,
    quarantined_count: int,
) -> dict[str, int]:
    """Count quarantined rows by rejection reason"""

    if quarantined_count == 0:
        return {}

    quarantine_df = spark.read.parquet(
        str(quarantine_path)
    )

    rows = (
        quarantine_df.groupBy(
            "rejection_reason"
        )
        .count()
        .collect()
    )

    return {
        row["rejection_reason"]: row["count"]
        for row in rows
    }


def create_pipeline_report(
    spark: SparkSession,
    extraction_result: ExtractionResult,
    transformation_result: TransformationResult,
    gold_result: GoldBuildResult,
    category: str,
    report_root: str,
) -> Path:
    """Write one pipeline report as formatted JSON"""

    quarantine_breakdown = get_quarantine_breakdown(
        spark=spark,
        quarantine_path=transformation_result.quarantine_path,
        quarantined_count=(
            transformation_result.quarantined_count
        ),
    )

    report = PipelineReport(
        run_id=extraction_result.run_id,
        status="success",
        generated_at=datetime.now(
            timezone.utc
        ).isoformat(),
        source="open_food_facts",
        category=category,
        bronze_product_path=str(
            extraction_result.product_path
        ),
        silver_path=str(
            transformation_result.silver_path
        ),
        quarantine_path=str(
            transformation_result.quarantine_path
        ),
        gold_path=str(
            gold_result.gold_run_path
        ),
        extracted_record_count=(
            extraction_result.record_count
        ),
        silver_record_count=(
            transformation_result.valid_count
        ),
        quarantined_record_count=(
            transformation_result.quarantined_count
        ),
        brand_summary_count=(
            gold_result.brand_summary_count
        ),
        nutrition_grade_summary_count=(
            gold_result.nutrition_grade_summary_count
        ),
        quarantine_breakdown=quarantine_breakdown,
    )

    report_directory = (
        Path(report_root)
        / f"run_id={report.run_id}"
    )

    report_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_path = (
        report_directory
        / "pipeline_report.json"
    )

    report_path.write_text(
        json.dumps(
            asdict(report),
            indent=2,
        ),
        encoding="utf-8",
    )

    return report_path