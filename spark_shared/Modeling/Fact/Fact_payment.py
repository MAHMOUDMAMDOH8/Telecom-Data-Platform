from utils import *
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def load_fact_payment(spark: SparkSession , clickhouse_base_options):
    try:
        spark.sql("CREATE OR REPLACE TEMP VIEW fact_payment AS SELECT * FROM local.payment")
        
        # Check which columns exist in the table
        df_check = spark.sql("SELECT * FROM fact_payment LIMIT 1")
        available_columns = set(df_check.columns)
        
        # Build column list with NULL handling
        select_cols = [
            "sid as payment_id",
            "event_type",
            "customer as customer_phone",
            "COALESCE(phone_number, '') as phone_number",
            "COALESCE(payment_type, '') as payment_type",
            "COALESCE(payment_amount, 0) as payment_amount",
            "COALESCE(payment_method, '') as payment_method",
            "COALESCE(status, '') as status",
            "COALESCE(CAST(timestamp AS timestamp), CAST('1970-01-01 00:00:00' AS timestamp)) as timestamp",
            "COALESCE(transaction_id, '') as transaction_id",
            "COALESCE(amount, 0) as amount",
            "COALESCE(currency, '') as currency"
        ]
        
        # Add optional columns if they exist
        if 'invoice_number' in available_columns:
            select_cols.append("COALESCE(invoice_number, '') as invoice_number")
        else:
            select_cols.append("'' as invoice_number")
            
        if 'seasonal_multiplier' in available_columns:
            select_cols.append("COALESCE(seasonal_multiplier, 1.0) as seasonal_multiplier")
        else:
            select_cols.append("1.0 as seasonal_multiplier")
        
        query = f"""
        with fact_payment_source as (
            select 
                {', '.join(select_cols)}
            from fact_payment
            where is_rejected is false or is_rejected is null
            )
        select 
            md5(COALESCE(event_type, '') || COALESCE(CAST(payment_id AS STRING), '') || COALESCE(customer_phone, '') || COALESCE(CAST(timestamp AS STRING), '')) as trasnaction_sk ,
            COALESCE(md5(date_format(timestamp, 'yyyy-MM-dd')), md5('1970-01-01')) as date_sk,
            COALESCE(EXTRACT(MINUTE FROM timestamp), 0) AS time_sk,
            payment_id,
            event_type,
            customer_phone,
            phone_number,
            payment_type,
            payment_amount,
            payment_method,
            status,
            timestamp,
            transaction_id,
            invoice_number,
            seasonal_multiplier,
            amount,
            currency
        from fact_payment_source
        """
        df = spark.sql(query)
        write_table_to_clickhouse(df, "fact_payment", clickhouse_base_options)
        logger.info("fact_payment loaded")
    except Exception as e:
        logger.error(f"Error in load_fact_payment: {e}", exc_info=True)
        raise