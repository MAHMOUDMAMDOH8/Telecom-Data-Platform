from utils import *
from pyspark.sql import SparkSession

import logging

logger = logging.getLogger(__name__)

def load_fact_recharge(spark: SparkSession , clickhouse_base_options):
    try:
        spark.sql("CREATE OR REPLACE TEMP VIEW fact_recharge AS SELECT * FROM local.recharge")
        query = """
        with fact_recharge_source as (
            select 
                sid as recharge_id ,
                COALESCE(transaction_id, '') as transaction_id,
                event_type ,
                customer as customer_phone,
                COALESCE(balance_after, 0) as balance_after,
                COALESCE(balance_before, 0) as balance_before,
                COALESCE(amount, 0) as amount,
                COALESCE(currency, '') as currency,
                COALESCE(CAST(timestamp AS timestamp), CAST('1970-01-01 00:00:00' AS timestamp)) as timestamp,
                COALESCE(payment_method, '') as payment_method,
                COALESCE(phone_number, '') as phone_number
            from fact_recharge
            where is_rejected is false or is_rejected is null
            )
        select 
            md5(COALESCE(event_type, '') || COALESCE(CAST(recharge_id AS STRING), '') || COALESCE(customer_phone, '') || COALESCE(CAST(timestamp AS STRING), '')) as trasnaction_sk ,
            COALESCE(md5(date_format(timestamp, 'yyyy-MM-dd')), md5('1970-01-01')) as date_sk,
            COALESCE(EXTRACT(MINUTE FROM timestamp), 0) AS time_sk,
            recharge_id,
            transaction_id,
            event_type,
            customer_phone,
            balance_after,
            balance_before,
            amount,
            currency,
            timestamp,
            payment_method,
            phone_number
         from fact_recharge_source
        """
        df = spark.sql(query)
        write_table_to_clickhouse(df, "fact_recharge", clickhouse_base_options)
        logger.info("fact_recharge loaded")
    except Exception as e:
        logger.error(f"Error in load_fact_recharge: {e}", exc_info=True)
        raise