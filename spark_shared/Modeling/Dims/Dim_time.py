from utils import *
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def load_dim_time(spark: SparkSession , clickhouse_base_options):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW time_series AS
        SELECT 
            explode(sequence(0, 1439, 1)) AS minute_of_day
    """)

    query = """
        SELECT
            minute_of_day AS time_key,
            CAST(minute_of_day AS STRING) AS time_value,
            CAST(FLOOR(minute_of_day / 60) AS STRING) AS hour,
            CAST(MOD(minute_of_day, 60) AS STRING) AS minute,
            CASE 
                WHEN FLOOR(minute_of_day / 60) < 12 THEN 'AM'
                ELSE 'PM'
            END AS am_pm
        FROM time_series
    """
    df = spark.sql(query)
    write_table_to_clickhouse(df, "dim_time", clickhouse_base_options)
    logger.info("dim_time loaded")