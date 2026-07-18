"""Integration test for the FoodLens transformation pipeline"""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.config import Settings
from src.transform import transform_products


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create one local Spark session for integration tests"""

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("FoodLensIntegrationTests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_bronze_to_silver_and_quarantine(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    """Verify valid and invalid sample records are separated"""

    settings = Settings(
        base_url="https://world.openfoodfacts.org",
        category="chocolates",
        page_size=10,
        max_pages=1,
        user_agent="FoodLensTests/1.0",
        bronze_root=str(tmp_path / "bronze"),
        silver_root=str(tmp_path / "silver"),
        gold_root=str(tmp_path / "gold"),
        quarantine_root=str(tmp_path / "quarantine"),
        report_root=str(tmp_path / "reports"),
    )

    sample_path = Path(
        "data/samples/products_sample.json"
    )

    result = transform_products(
        spark=spark,
        settings=settings,
        bronze_product_path=sample_path,
        run_id="integration-test-run",
    )

    assert result.source_count == 3
    assert result.valid_count == 2
    assert result.quarantined_count == 1

    silver_df = spark.read.parquet(
        str(result.silver_path)
    )

    quarantine_df = spark.read.parquet(
        str(result.quarantine_path)
    )

    assert silver_df.count() == 2
    assert quarantine_df.count() == 1

    rejected_row = quarantine_df.select(
        "rejection_reason"
    ).first()

    assert rejected_row is not None
    assert "missing_barcode" in rejected_row[
        "rejection_reason"
    ]
    assert "invalid_sugars_100g" in rejected_row[
        "rejection_reason"
    ]