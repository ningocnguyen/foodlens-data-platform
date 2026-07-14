"""Generate JSON reports for completed FoodLens pipeline runs"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path

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


def create_pipeline_report(
    extraction_result: ExtractionResult,
    transformation_result: TransformationResult,
    gold_result: GoldBuildResult,
    category: str,
    report_root: str,
) -> Path:
    """Write one pipeline report as formatted JSON"""

    report = PipelineReport(
        run_id=extraction_result.run_id,
        status="success",
        generated_at=datetime.now(timezone.utc).isoformat(),
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