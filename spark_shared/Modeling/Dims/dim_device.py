from utils import *
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def load_dim_device(spark: SparkSession , clickhouse_base_options):

    spark.sql("CREATE OR REPLACE TEMP VIEW dim_device AS SELECT * FROM local.dim_device")

    query = """
    with dim_device_source as (
        select 
            distinct tac as tac,
            brand as brand,
            model as model
        from dim_device )
    select md5(tac || brand ) as device_sk , * from dim_device_source
    """
    df = spark.sql(query)
    write_table_to_clickhouse(df, "dim_device", clickhouse_base_options)
