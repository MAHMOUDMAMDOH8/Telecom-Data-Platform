def sms_schema():
    return [
        'event_type','sid', 'timestamp',
        'from', 'to', 'status',
        'body', 'billing_info', 'customer_id', 'network_metrics'
    ]

    
def call_schema():
    return [
        'event_type','sid', 'timestamp', 
        'from', 'to', 'status','call_duration_seconds' ,
        'billing_info', 'call_type'
    ]



def payment_schema():
    return [
        'event_type','sid', 'customer', 'payment_type',
        'payment_amount', 'payment_method', 'status',
        'timestamp', 'phone_number', 'transaction_id',
        'invoice_number', 'seasonal_multiplier', 'billing_info'
    ]



def recharge_schema():
    return [
        'event_type','sid', 'timestamp',
        'from', 'to', 'status',
        'body', 'billing_info', 'customer', 'phone_number',
        'recharge_amount', 'requires_followup', 'seasonal_multiplier',
        'sid', 'status', 'timestamp', 'transaction_id'
    ]



def support_schema():
    return [
        'event_type','sid', 'customer', 'channel',
         'reason', 'wait_time_seconds', 'resolution_time_seconds',
          'agent_id', 'satisfaction_score', 'timestamp', 'phone_number',
           'first_call_resolution', 'escalated', 'call_back_requested'
    ]