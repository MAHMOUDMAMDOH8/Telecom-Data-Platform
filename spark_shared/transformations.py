
from typing import Optional

from pyspark.sql.functions import col, concat
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, lit
from pyspark.sql.types import StructType


def normalize_columns(df: DataFrame, columns: str, prefix: str) -> DataFrame:
    if columns not in df.columns:
        return df

    data_type = df.schema[columns].dataType
    if not isinstance(data_type, StructType):
        return df

    fields = data_type.names
    for field in fields:
        new_col_name = f"{prefix}_{field}" if prefix else field
        df = df.withColumn(new_col_name, col(columns)[field])
    return df.drop(columns)

def add_rejection_reason(
    df: DataFrame,
    required_columns: list,
    numeric_columns: Optional[list] = None,
    positive_columns: Optional[list] = None,
    is_between_columns: Optional[dict] = None,
    ) -> DataFrame:


    df = df.withColumn("rejection_reason", lit(""))

    for col_name in required_columns:
        df = df.withColumn(
            "rejection_reason",
            when(
                col(col_name).isNull(),
                concat(col("rejection_reason"), lit(f"{col_name} come with null value, ")),
            ).otherwise(col("rejection_reason"))
        )

    if numeric_columns:
        for col_name in numeric_columns:
            df = df.withColumn(
                "rejection_reason",
                when(
                    ~col(col_name).cast("double").isNotNull(),
                    concat(col("rejection_reason"), lit(f"{col_name} must be numeric, ")),
                ).otherwise(col("rejection_reason"))
            )

    if positive_columns:
        for col_name in positive_columns:
            df = df.withColumn(
                "rejection_reason",
                when(
                    col(col_name).cast("double") <= 0,
                    concat(col("rejection_reason"), lit(f"{col_name} must be positive, ")),
                ).otherwise(col("rejection_reason"))
            )

    if is_between_columns:
        for col_name, (min_val, max_val) in is_between_columns.items():
            df = df.withColumn(
                "rejection_reason",
                when(
                    (col(col_name).cast("double") < min_val)
                    | (col(col_name).cast("double") > max_val),
                    concat(
                        col("rejection_reason"),
                        lit(f"{col_name} must be between {min_val} and {max_val}, "),
                    ),
                ).otherwise(col("rejection_reason"))
            )

    df = df.withColumn(
        "rejection_reason",
        when(col("rejection_reason") == "", None)
        .otherwise(col("rejection_reason").substr(1, 1000))
    )

    df = df.withColumn(
        "is_rejected",
        col("rejection_reason").isNotNull()
    )

    return df