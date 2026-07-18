"""Run the complete FoodLens data pipeline"""

from __future__ import annotations

import logging
import sys

from src.build_gold import build_gold_datasets
from src.config import get_settings
from src.extract import extract_products
from src.reporting import create_pipeline_report
from src.transform import (
    create_spark_session,
    transform_products,
)


def configure_logging() -> None:
    """Configure readable application logs"""

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    logging.getLogger("py4j").setLevel(
        logging.WARNING
    )


def main() -> int:
    """Execute the complete Bronze, Silver, and Gold pipeline"""

    configure_logging()

    logger = logging.getLogger(
        "foodlens.pipeline"
    )

    spark = None

    try:
        settings = get_settings()

        logger.info(
            "Starting FoodLens pipeline"
        )

        logger.info(
            "Category: %s",
            settings.category,
        )

        logger.info(
            "Stage 1/4: extracting Bronze data"
        )

        extraction_result = extract_products(
            settings
        )

        logger.info(
            "Bronze extraction completed: records=%s",
            extraction_result.record_count,
        )

        logger.info(
            "Stage 2/4: transforming Silver data"
        )

        spark = create_spark_session()

        transformation_result = transform_products(
            spark=spark,
            settings=settings,
            bronze_product_path=(
                extraction_result.product_path
            ),
            run_id=extraction_result.run_id,
        )

        logger.info(
            (
                "Silver transformation completed: "
                "valid=%s quarantined=%s"
            ),
            transformation_result.valid_count,
            transformation_result.quarantined_count,
        )

        logger.info(
            "Stage 3/4: building Gold datasets"
        )

        gold_result = build_gold_datasets(
            spark=spark,
            settings=settings,
            silver_path=(
                transformation_result.silver_path
            ),
            run_id=extraction_result.run_id,
            source_count=(
                transformation_result.source_count
            ),
            quarantined_count=(
                transformation_result.quarantined_count
            ),
        )

        logger.info(
            "Gold build completed: path=%s",
            gold_result.gold_run_path,
        )

        logger.info(
            "Stage 4/4: creating pipeline report"
        )

        report_path = create_pipeline_report(
          spark=spark,
          extraction_result=extraction_result,
          transformation_result=transformation_result,
          gold_result=gold_result,
          category=settings.category,
          report_root=settings.report_root,
        )     

        logger.info(
            "Pipeline report created: %s",
            report_path,
        )

        print()
        print("FoodLens pipeline completed successfully")
        print(
            f"Run ID: {extraction_result.run_id}"
        )
        print(
            "Extracted records: "
            f"{extraction_result.record_count}"
        )
        print(
            "Silver records: "
            f"{transformation_result.valid_count}"
        )
        print(
            "Quarantined records: "
            f"{transformation_result.quarantined_count}"
        )
        print(
            f"Gold path: {gold_result.gold_run_path}"
        )
        print(
            f"Report path: {report_path}"
        )

        return 0

    except Exception:
        logger.exception(
            "FoodLens pipeline failed"
        )

        return 1

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())