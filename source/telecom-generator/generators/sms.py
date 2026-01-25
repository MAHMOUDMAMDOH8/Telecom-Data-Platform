import random
from datetime import datetime, timedelta

from .base import apply_seasonal_patterns, generate_user_info, introduce_data_quality_issues, sample_messages


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

    # Generate user info for 'from' (sender) with phone_number, cell_site, and imei
    from_user_info = generate_user_info(customers, cell_sites, allow_data_issues=allow_data_issues, force_clean=force_clean)
    
    # Generate user info for 'to' (receiver) with phone_number, cell_site, and imei
    to_user_info = generate_user_info(customers, cell_sites, allow_data_issues=allow_data_issues, force_clean=force_clean)

    # Build 'from' object with phone_number, cell_site, and imei
    from_data = {
        "phone_number": from_user_info["number"],
        "cell_site": from_user_info["cell_site"],
        "imei": from_user_info["imei"]
    }
    
    # Build 'to' object with phone_number, cell_site, and imei
    to_data = {
        "phone_number": to_user_info["number"],
        "cell_site": to_user_info["cell_site"],
        "imei": to_user_info["imei"]
    }

    event = {
        "event_type": "sms",
        "sid": f"SM{random.randint(10**8, 10**9)}",
        "from": from_data,
        "to": to_data,
        "body": random.choice(sample_messages),
        "status": sms_status,
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "phone_number": from_user_info["number"],  # Keep for backward compatibility
        "customer": from_user_info["number"],  # Keep for backward compatibility
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


