from init import * 
from transformations import *
from scheam import *


spark = get_spark_session(app_name="from_bronze_to_silver",
   s3_bucket='s3a://telecom_lakehouse/',
    s3_endpoint='https://expert-pancake-jv9wx6vww5w25gj4-4566.app.github.dev/',
    access_key='test',
    secret_key='test')




def calls_transformation():

    df = read_from_bronze('call', call_schema(), spark=spark)
    df = normalize_columns(df, 'from', 'from_')
    df = normalize_columns(df, 'to', 'to_')
    df = normalize_columns(df, 'billing_info', '')

    final_df = add_rejection_reason(df,
    required_columns=['event_type', 'sid', 'timestamp', 'status',
                     'from_phone_number', 'to_phone_number', 
                     'call_duration_seconds', 'amount', 'currency', 'call_type'
                    ],
    numeric_columns=['call_duration_seconds', 'amount'],
    positive_columns=['amount'],
    is_between_columns={'call_duration_seconds': (0, 3600)})

    write_to_iceberg(final_df, 'calls')


def sms_transformation():

    df = read_from_bronze('sms', sms_schema(), spark=spark)
    df = normalize_columns(df, 'from', 'from_')
    df = normalize_columns(df, 'to', 'to_')
    df = normalize_columns(df, 'billing_info', '')
    df = normalize_columns(df, 'network_metrics', '')


    final_df = add_rejection_reason(df,
    required_columns=['event_type', 'sid', 'timestamp', 'status',
                     'from_phone_number', 'to_phone_number', 
                     'from_cell_site', 'to_cell_site',
                     'from_imei', 'to_imei',
                     'body', 'currency', 'amount'
                    ],
    numeric_columns=['amount'],
    positive_columns=['amount']

    write_to_iceberg(final_df, 'sms')


def payment_transformation():
    df = read_from_bronze('payment', payment_schema(), spark=spark)
    df = normalize_columns(df, 'billing_info', '')
    final_df = add_rejection_reason(df,
    required_columns=['event_type', 'sid', 'timestamp', 'status',
                     'customer', 'payment_type', 'payment_amount', 'payment_method',
                     'phone_number', 'transaction_id', 'invoice_number', 'seasonal_multiplier', 'amount', 'currency'
                    ],
    numeric_columns=['payment_amount', 'amount'],
    positive_columns=['payment_amount', 'amount']
    )

    write_to_iceberg(final_df, 'payment')

