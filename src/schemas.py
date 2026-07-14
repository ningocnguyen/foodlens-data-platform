"""Spark schemas used by the FoodLens pipeline"""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


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