from utils import *
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def load_dim_user(spark: SparkSession , clickhouse_base_options):

    spark.sql("CREATE OR REPLACE TEMP VIEW dim_user AS SELECT * FROM local.dim_user")

    query = """
    with dim_user_source as (
        select 
            distinct user_sk as user_id,
            msisdn as phone_number,
            customer_type as plan_type,
            gender,
            age_group as age_group,
            city as city,
            activation_date as activation_date,
            status as status,
            effective_from as effective_from,
            effective_to as effective_to,
            is_current as is_current
        from dim_user )
    select md5(user_id || phone_number ) as user_sk , * from dim_user_source
    """

    df = spark.sql(query)


    write_table_to_clickhouse(df, "dim_user", clickhouse_base_options)

    logger.info("dim_user loaded")



