from utils import *
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def load_dim_cell_site(spark: SparkSession , clickhouse_base_options):

    spark.sql("CREATE OR REPLACE TEMP VIEW dim_cell_site AS SELECT * FROM local.dim_cell_site")

    query = """
    with dim_cell_site_source as (
        select 
            distinct cell_id as cell_id,
            city as city,
            site_name as site_name,
            latitude as latitude,
            longitude as longitude
        from dim_cell_site
    )
    select md5(cell_id || city ) as cell_sk , * from dim_cell_site_source
    """

    df = spark.sql(query)

    write_table_to_clickhouse(df, "dim_cell_site", clickhouse_base_options)

    logger.info("dim_cell_site loaded")

