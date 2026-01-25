import random
from datetime import datetime, timedelta

from .base import apply_seasonal_patterns, get_customer_phone_number, introduce_data_quality_issues, sample_messages


def generate_sms_event(customers, cell_sites, base_time=None, allow_data_issues=True, force_clean=False):
    """Generate SMS event with realistic patterns and data quality issues."""
    sms_status = random.choice(["queued", "sending", "sent", "delivered", "failed"])

    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time = base_time + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    seasonal_mult = apply_seasonal_patterns(base_time)

    if sms_status == "delivered":
        billing_amount = round(random.uniform(0.10, 0.50) * seasonal_mult, 2)
    elif sms_status == "sent":
        billing_amount = round(random.uniform(0.05, 0.25) * seasonal_mult, 2)
    else:
        billing_amount = 0.0

    from_phone = get_customer_phone_number(customers, allow_data_issues=allow_data_issues, force_clean=force_clean)
    to_phone = get_customer_phone_number(customers, allow_data_issues=allow_data_issues, force_clean=force_clean)

    if random.random() < 0.3:
        to_phone = get_customer_phone_number(customers, allow_data_issues=allow_data_issues, force_clean=force_clean)

    event = {
        "event_type": "sms",
        "sid": f"SM{random.randint(10**8, 10**9)}",
        "from": from_phone,
        "to": to_phone,
        "body": random.choice(sample_messages),
        "status": sms_status,
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "phone_number": from_phone,
        "customer": from_phone,
        "registration_date": (base_time - timedelta(days=random.randint(1, 1095))).strftime("%Y-%m-%d"),
        "seasonal_multiplier": round(seasonal_mult, 2),
        "billing_info": {"amount": billing_amount, "currency": "EGP"},
    }

    if random.random() < 0.5:
        from .base import generate_network_metrics

        event["network_metrics"] = generate_network_metrics()

    if allow_data_issues:
        introduce_data_quality_issues(event, "sms", force_clean=force_clean)

    return event


