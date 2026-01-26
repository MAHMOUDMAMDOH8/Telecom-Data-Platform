
from pyspark.sql.functions import col, concat
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, lit


def normalize_columns(df: DataFrame, columns: str, prefix: str) -> DataFrame:
    fields = df.schema[columns].dataType.names
    for field in fields:
        new_col_name = f"{prefix}_{field}" if prefix else field
        df = df.withColumn(new_col_name, col(columns)[field])
    return df.drop(columns)

def add_rejection_reason(
    df: DataFrame,
    required_columns: list,
    numeric_columns: list | None = None,
    positive_columns: list | None = None,
    is_between_columns: dict | None = None,
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