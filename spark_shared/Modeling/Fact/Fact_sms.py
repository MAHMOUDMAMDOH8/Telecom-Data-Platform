from utils import *
from pyspark.sql import SparkSession

import logging

logger = logging.getLogger(__name__)

def load_fact_sms(spark: SparkSession , clickhouse_base_options):
    try:
        spark.sql("CREATE OR REPLACE TEMP VIEW fact_sms AS SELECT * FROM local.sms")
        
        # Check which columns exist in the table
        df_check = spark.sql("SELECT * FROM fact_sms LIMIT 1")
        available_columns = set(df_check.columns)
        
        # Build column list with NULL handling
        select_cols = [
            "sid as sms_id",
            "event_type",
            "COALESCE(from_phone_number, '') as from_phone_number",
            "COALESCE(to_phone_number, '') as to_phone_number",
            "COALESCE(from_imei, '') as from_imei",
            "COALESCE(to_imei, '') as to_imei",
            "CASE WHEN from_imei IS NOT NULL AND length(from_imei) >= 8 THEN substr(from_imei,1,8) ELSE '' END as from_tac",
            "CASE WHEN to_imei IS NOT NULL AND length(to_imei) >= 8 THEN substr(to_imei,1,8) ELSE '' END as to_tac",
            "COALESCE(from_cell_site, '') as from_cell_site",
            "COALESCE(to_cell_site, '') as to_cell_site",
            "COALESCE(CAST(timestamp AS timestamp), CAST('1970-01-01 00:00:00' AS timestamp)) as timestamp",
            "COALESCE(status, '') as status",
            "COALESCE(amount, 0) as amount",
            "COALESCE(currency, '') as currency"
        ]
        
        # Add optional columns if they exist
        if 'jitter_ms' in available_columns:
            select_cols.append("COALESCE(jitter_ms, 0) as jitter_ms")
        else:
            select_cols.append("0 as jitter_ms")
            
        if 'latency_ms' in available_columns:
            select_cols.append("COALESCE(latency_ms, 0) as latency_ms")
        else:
            select_cols.append("0 as latency_ms")
            
        if 'signal_strength_dbm' in available_columns:
            select_cols.append("COALESCE(signal_strength_dbm, 0) as signal_strength_dbm")
        else:
            select_cols.append("0 as signal_strength_dbm")
        
        query = f"""
        with fact_sms_source as (
            select 
                {', '.join(select_cols)}
            from fact_sms
            where is_rejected is false or is_rejected is null
            )
        select 
            md5(COALESCE(event_type, '') || COALESCE(CAST(sms_id AS STRING), '') || COALESCE(from_phone_number, '') || COALESCE(to_phone_number, '') || COALESCE(CAST(timestamp AS STRING), '')) as trasnaction_sk ,
            COALESCE(md5(date_format(timestamp, 'yyyy-MM-dd')), md5('1970-01-01')) as date_sk,
            COALESCE(EXTRACT(MINUTE FROM timestamp), 0) AS time_sk,
            sms_id,
            event_type,
            from_phone_number,
            to_phone_number,
            from_imei,
            to_imei,
            from_tac,
            to_tac,
            from_cell_site,
            to_cell_site,
            timestamp,
            status,
            jitter_ms,
            latency_ms,
            signal_strength_dbm,
            amount,
            currency
        from fact_sms_source
        """

        df = spark.sql(query)
        write_table_to_clickhouse(df, "fact_sms", clickhouse_base_options)
        logger.info("fact_sms loaded")
    except Exception as e:
        logger.error(f"Error in load_fact_sms: {e}", exc_info=True)
        raise

