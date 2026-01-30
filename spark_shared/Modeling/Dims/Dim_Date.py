from utils import *
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def load_dim_date(spark: SparkSession , clickhouse_base_options):
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW date_series AS
        SELECT explode(sequence(to_date('2025-01-01'), to_date('2025-12-31'), interval 1 day)) AS date_value
     """)
    query = """
    with date_series_source as (
        select 
            date_value as full_date,
            date_format(date_value, 'yyyy-MM-dd') as date,
            date_format(date_value, 'yyyy') as year,
            date_format(date_value, 'MM') as month,
            date_format(date_value, 'dd') as day
        from date_series
    )
    select md5(date) as date_sk , * from date_series_source
    """
    df = spark.sql(query)
    write_table_to_clickhouse(df, "dim_date", clickhouse_base_options)
    logger.info("dim_date loaded")
