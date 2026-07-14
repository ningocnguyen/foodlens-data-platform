"""Data-quality rules for FoodLens product records"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def missing_barcode_rule() -> Column:
    """Return True when the barcode is missing or blank"""

    return (
        F.col("barcode").isNull()
        | (F.trim(F.col("barcode")) == "")
    )


def missing_product_name_rule() -> Column:
    """Return True when the product name is missing or blank"""

    return (
        F.col("product_name").isNull()
        | (F.trim(F.col("product_name")) == "")
    )


def invalid_nutrition_rule(column_name: str) -> Column:
    """Return True when a nutrition value is outside 0–100 grams"""

    return (
        F.col(column_name).isNotNull()
        & (
            (F.col(column_name) < 0)
            | (F.col(column_name) > 100)
        )
    )


def invalid_energy_rule() -> Column:
    """Return True when calories per 100g are outside a reasonable range"""

    return (
        F.col("energy_kcal_100g").isNotNull()
        & (
            (F.col("energy_kcal_100g") < 0)
            | (F.col("energy_kcal_100g") > 1000)
        )
    )


def build_rejection_reason() -> Column:
    """Create a readable reason explaining why a row is invalid"""

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