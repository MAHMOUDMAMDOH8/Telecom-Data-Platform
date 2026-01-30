from utils import *
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def load_fact_calls(spark: SparkSession , clickhouse_base_options):
    try:
        spark.sql("CREATE OR REPLACE TEMP VIEW fact_calls AS SELECT * FROM local.calls")
        query = """
        with fact_calls_source as (
            select 
                sid as call_id ,
                event_type ,
                COALESCE(from_phone_number, '') as from_phone_number ,
                COALESCE(to_phone_number, '') as to_phone_number ,
                COALESCE(from_cell_site, '') as from_cell_site ,
                COALESCE(to_cell_site, '') as to_cell_site ,
                COALESCE(from_imei, '') as from_imei,
                COALESCE(to_imei, '') as to_imei,
                CASE WHEN from_imei IS NOT NULL AND length(from_imei) >= 8 THEN substr(from_imei,1,8) ELSE '' END as from_tac,
                CASE WHEN to_imei IS NOT NULL AND length(to_imei) >= 8 THEN substr(to_imei,1,8) ELSE '' END as to_tac,
                COALESCE(CAST(timestamp AS timestamp), CAST('1970-01-01 00:00:00' AS timestamp)) as timestamp,
                COALESCE(status, '') as status ,
                COALESCE(call_duration_seconds, 0) as call_duration_seconds ,
                COALESCE(amount, 0) as amount,
                COALESCE(currency, '') as currency,
                COALESCE(call_type, '') as call_type
            from fact_calls
            where is_rejected is false or is_rejected is null
        )
        select 
            md5(COALESCE(event_type, '') || COALESCE(CAST(call_id AS STRING), '') || COALESCE(from_phone_number, '') || COALESCE(to_phone_number, '') || COALESCE(CAST(timestamp AS STRING), '')) as trasnaction_sk ,
            COALESCE(md5(date_format(timestamp, 'yyyy-MM-dd')), md5('1970-01-01')) as date_sk,
            COALESCE(EXTRACT(MINUTE FROM timestamp), 0) AS time_sk,
            call_id,
            event_type,
            from_phone_number,
            to_phone_number,
            from_cell_site,
            to_cell_site,
            from_imei,
            to_imei,
            from_tac,
            to_tac,
            timestamp,
            status,
            call_duration_seconds,
            amount,
            currency,
            call_type
        from fact_calls_source
        """
        df = spark.sql(query)
        write_table_to_clickhouse(df, "fact_calls", clickhouse_base_options)
        logger.info("fact_calls loaded")
    except Exception as e:
        logger.error(f"Error in load_fact_calls: {e}", exc_info=True)
        raise