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
    move_to_archive(df, 'calls')


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
    )

    write_to_iceberg(final_df, 'sms')
    move_to_archive(df, 'sms')


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
    move_to_archive(df, 'payment')


def recharge_transformation():
    df = read_from_bronze('recharge', recharge_schema(), spark=spark)
    df = normalize_columns(df, 'billing_info', '')
    
    final_df = add_rejection_reason(df,
    required_columns=['event_type', 'sid', 'timestamp', 'status',
                     'customer', 'phone_number', 'recharge_amount', 
                     'balance_before', 'balance_after', 'payment_method',
                     'transaction_id', 'amount', 'currency'
                    ],
    numeric_columns=['recharge_amount', 'balance_before', 'balance_after', 'amount'],
    positive_columns=['recharge_amount', 'amount']
    )

    write_to_iceberg(final_df, 'recharge')
    move_to_archive(df, 'recharge')


def support_transformation():
    df = read_from_bronze('support', support_schema(), spark=spark)
    
    # Build is_between_columns conditionally
    is_between_cols = {}
    if 'satisfaction_score' in df.columns:
        is_between_cols['satisfaction_score'] = (1, 5)
    
    final_df = add_rejection_reason(df,
    required_columns=['event_type', 'sid', 'timestamp', 'status',
                     'customer', 'phone_number', 'channel', 'reason',
                     'wait_time_seconds', 'resolution_time_seconds', 'agent_id'
                    ],
    numeric_columns=['wait_time_seconds', 'resolution_time_seconds', 'satisfaction_score'],
    positive_columns=['wait_time_seconds', 'resolution_time_seconds'],
    is_between_columns=is_between_cols if is_between_cols else None
    )

    write_to_iceberg(final_df, 'support')
    move_to_archive(df, 'support')


def process_all_transformations():
    """Process all event types from bronze to silver layer."""
    transformations = {
        'call': calls_transformation,
        'sms': sms_transformation,
        'payment': payment_transformation,
        'recharge': recharge_transformation,
        'support': support_transformation
    }
    
    for event_type, transformation_func in transformations.items():
        try:
            print(f"\n{'='*50}")
            print(f"Processing {event_type} events...")
            print(f"{'='*50}")
            transformation_func()
            print(f"Successfully processed {event_type} events")
        except Exception as e:
            print(f"Error processing {event_type} events: {e}")
            continue


if __name__ == "__main__":

    process_all_transformations()


