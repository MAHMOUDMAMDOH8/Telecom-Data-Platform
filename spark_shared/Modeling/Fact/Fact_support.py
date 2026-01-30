from utils import *
from pyspark.sql import SparkSession

import logging

logger = logging.getLogger(__name__)

def load_fact_support(spark: SparkSession , clickhouse_base_options):
    try:
        spark.sql("CREATE OR REPLACE TEMP VIEW fact_support AS SELECT * FROM local.support")
        
        # Check which columns exist in the table
        df_check = spark.sql("SELECT * FROM fact_support LIMIT 1")
        available_columns = set(df_check.columns)
        
        # Build column list with NULL handling
        select_cols = [
            "sid as support_id",
            "event_type",
            "customer as customer_phone",
            "COALESCE(phone_number, '') as phone_number",
            "COALESCE(channel, '') as channel",
            "COALESCE(reason, '') as reason",
            "COALESCE(wait_time_seconds, 0) as wait_time_seconds",
            "COALESCE(resolution_time_seconds, 0) as resolution_time_seconds",
            "COALESCE(agent_id, '') as agent_id",
            "COALESCE(CAST(timestamp AS timestamp), CAST('1970-01-01 00:00:00' AS timestamp)) as timestamp",
            "COALESCE(escalated, false) as escalated"
        ]
        
        # Add optional columns if they exist
        if 'satisfaction_score' in available_columns:
            select_cols.append("COALESCE(satisfaction_score, 0) as satisfaction_score")
        else:
            select_cols.append("0 as satisfaction_score")
            
        if 'first_call_resolution' in available_columns:
            select_cols.append("COALESCE(first_call_resolution, false) as first_call_resolution")
        else:
            select_cols.append("false as first_call_resolution")
            
        if 'call_back_requested' in available_columns:
            select_cols.append("COALESCE(call_back_requested, false) as call_back_requested")
        else:
            select_cols.append("false as call_back_requested")
        
        query = f"""
        with fact_support_source as (
            select 
                {', '.join(select_cols)}
            from fact_support
            where is_rejected is false or is_rejected is null
            )
        select 
            md5(COALESCE(event_type, '') || COALESCE(CAST(support_id AS STRING), '') || COALESCE(customer_phone, '') || COALESCE(CAST(escalated AS STRING), '') || COALESCE(CAST(timestamp AS STRING), '')) as trasnaction_sk ,
            COALESCE(md5(date_format(timestamp, 'yyyy-MM-dd')), md5('1970-01-01')) as date_sk,
            COALESCE(EXTRACT(MINUTE FROM timestamp), 0) AS time_sk,
            support_id,
            event_type,
            customer_phone,
            phone_number,
            channel,
            reason,
            wait_time_seconds,
            resolution_time_seconds,
            agent_id,
            satisfaction_score,
            timestamp,
            first_call_resolution,
            escalated,
            call_back_requested
        from fact_support_source
        """
        df = spark.sql(query)
        write_table_to_clickhouse(df, "fact_support", clickhouse_base_options)
        logger.info("fact_support loaded")
    except Exception as e:
        logger.error(f"Error in load_fact_support: {e}", exc_info=True)
        raise