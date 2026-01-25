import random
from datetime import datetime, timedelta

from .base import (
    BEHAVIOR_PROFILES,
    FRAUD_PATTERNS,
    apply_seasonal_patterns,
    generate_user_info,
    get_customer_phone_number,
    introduce_data_quality_issues,
)


def generate_call_event(customers, cell_sites, base_time=None, allow_data_issues=True, force_clean=False):
    """Generate call event with realistic patterns and data quality issues."""
    customer_sample = random.choice(customers)
    behavior_profile = customer_sample.get("behavior_profile", "light_user")
    profile = BEHAVIOR_PROFILES.get(behavior_profile, BEHAVIOR_PROFILES["light_user"])

    call_status = random.choice(["initiated", "ringing", "in-progress", "completed", "failed", "busy", "no-answer"])

    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time = base_time + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    seasonal_mult = apply_seasonal_patterns(base_time)

    call_weight = 2.0 if behavior_profile == "business_user" and 8 <= base_time.hour <= 18 else 1.0

    if call_status == "completed":
        base_duration = random.randint(30, 3600)
        call_duration_sec = int(base_duration * profile.get("call_multiplier", 1.0) * call_weight)
        billing_amount = round((call_duration_sec / 60) * 0.16 * seasonal_mult, 2)
    elif call_status == "in-progress":
        call_duration_sec = random.randint(1, 300)
        billing_amount = round((call_duration_sec / 60) * 0.16 * seasonal_mult, 2)
    elif call_status == "ringing":
        call_duration_sec = random.randint(1, 30)
        billing_amount = 0.0
    else:
        call_duration_sec = 0
        billing_amount = 0.0

    from_phone = get_customer_phone_number(customers, allow_data_issues=allow_data_issues, force_clean=force_clean)
    to_phone = get_customer_phone_number(customers, allow_data_issues=allow_data_issues, force_clean=force_clean)

    call_type = "International" if behavior_profile == "international" and random.random() < 0.3 else "Local"

    event = {
        "event_type": "call",
        "sid": f"CA{random.randint(10**8, 10**9)}",
        "from": from_phone,
        "to": to_phone,
        "call_duration_seconds": call_duration_sec,
        "status": call_status,
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "call_type": call_type,
        "phone_number": from_phone,
        "customer": from_phone,
        "behavior_profile": behavior_profile,
        "seasonal_multiplier": round(seasonal_mult, 2),
        "billing_info": {"amount": billing_amount, "currency": "EGP"},
    }

    event["qos_metrics"] = {
        "mos_score": round(random.uniform(1.0, 5.0), 2),
        "jitter_ms": round(random.uniform(0, 50), 2),
        "packet_loss_percent": round(random.uniform(0, 5), 2),
        "codec": random.choice(["AMR", "EVS", "OPUS", "G.711"]),
    }

    if allow_data_issues:
        introduce_data_quality_issues(event, "call", force_clean=force_clean)
        if not force_clean and random.random() < 0.04:
            event["call_duration_seconds"] = None if random.random() < 0.5 else -10

    if call_type == "International" and call_duration_sec < 30 and random.random() < 0.05:
        event["fraud_indicator"] = FRAUD_PATTERNS["international_short_calls"]["marker"]
        event["risk_score"] = random.randint(70, 95)

    return event


