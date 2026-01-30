from utils import *
from pyspark.sql import SparkSession

import logging

logger = logging.getLogger(__name__)

def load_dim_agent(spark: SparkSession , clickhouse_base_options):
    spark.sql("CREATE OR REPLACE TEMP VIEW dim_agent AS SELECT * FROM local.dim_agent")

    query = """
    with dim_agent_source as (
        select 
            distinct agent_id as agent_id,
            name as name,
            department as department,
            city as city,
            status as status
        from dim_agent )
    select md5(agent_id || name || department ) as agent_sk , * from dim_agent_source
    """
    df = spark.sql(query)
    write_table_to_clickhouse(df, "dim_agent", clickhouse_base_options)
    logger.info("dim_agent loaded")