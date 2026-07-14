"""Tests for FoodLens data-quality rules"""

import pytest
from pyspark.sql import SparkSession

from src.quality import build_rejection_reason


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create one local Spark session for all tests"""

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("FoodLensTests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    yield session

    session.stop()


def test_missing_barcode_is_rejected(
    spark: SparkSession,
) -> None:
    """A product without a barcode should be rejected"""

    rows = [
        {
            "barcode": None,
            "product_name": "Test Chocolate",
            "sugars_100g": 20.0,
            "fat_100g": 10.0,
            "proteins_100g": 5.0,
            "salt_100g": 0.1,
            "energy_kcal_100g": 400.0,
        }
    ]

    schema = """
        barcode string,
        product_name string,
        sugars_100g double,
        fat_100g double,
        proteins_100g double,
        salt_100g double,
        energy_kcal_100g double
    """

    dataframe = spark.createDataFrame(
        rows,
        schema=schema,
    )

    result = (
        dataframe.withColumn(
            "rejection_reason",
            build_rejection_reason(),
        )
        .select("rejection_reason")
        .first()
    )

    assert result is not None
    assert "missing_barcode" in result["rejection_reason"]


def test_missing_product_name_is_rejected(
    spark: SparkSession,
) -> None:
    """A product without a name should be rejected"""

    rows = [
        {
            "barcode": "123456789",
            "product_name": None,
            "sugars_100g": 20.0,
            "fat_100g": 10.0,
            "proteins_100g": 5.0,
            "salt_100g": 0.1,
            "energy_kcal_100g": 400.0,
        }
    ]

    schema = """
        barcode string,
        product_name string,
        sugars_100g double,
        fat_100g double,
        proteins_100g double,
        salt_100g double,
        energy_kcal_100g double
    """

    dataframe = spark.createDataFrame(
        rows,
        schema=schema,
    )

    result = (
        dataframe.withColumn(
            "rejection_reason",
            build_rejection_reason(),
        )
        .select("rejection_reason")
        .first()
    )

    assert result is not None
    assert "missing_product_name" in result["rejection_reason"]


def test_invalid_sugar_value_is_rejected(
    spark: SparkSession,
) -> None:
    """Sugar above 100 grams per 100 grams is invalid"""

    rows = [
        {
            "barcode": "123456789",
            "product_name": "Test Chocolate",
            "sugars_100g": 150.0,
            "fat_100g": 10.0,
            "proteins_100g": 5.0,
            "salt_100g": 0.1,
            "energy_kcal_100g": 400.0,
        }
    ]

    schema = """
        barcode string,
        product_name string,
        sugars_100g double,
        fat_100g double,
        proteins_100g double,
        salt_100g double,
        energy_kcal_100g double
    """

    dataframe = spark.createDataFrame(
        rows,
        schema=schema,
    )

    result = (
        dataframe.withColumn(
            "rejection_reason",
            build_rejection_reason(),
        )
        .select("rejection_reason")
        .first()
    )

    assert result is not None
    assert "invalid_sugars_100g" in result["rejection_reason"]


def test_valid_product_has_no_rejection_reason(
    spark: SparkSession,
) -> None:
    """A valid product should pass all quality checks"""

    rows = [
        {
            "barcode": "123456789",
            "product_name": "Test Chocolate",
            "sugars_100g": 20.0,
            "fat_100g": 10.0,
            "proteins_100g": 5.0,
            "salt_100g": 0.1,
            "energy_kcal_100g": 400.0,
        }
    ]

    schema = """
        barcode string,
        product_name string,
        sugars_100g double,
        fat_100g double,
        proteins_100g double,
        salt_100g double,
        energy_kcal_100g double
    """

    dataframe = spark.createDataFrame(
        rows,
        schema=schema,
    )

    result = (
        dataframe.withColumn(
            "rejection_reason",
            build_rejection_reason(),
        )
        .select("rejection_reason")
        .first()
    )

    assert result is not None
    assert result["rejection_reason"] == ""