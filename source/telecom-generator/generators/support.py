import random
from datetime import datetime, timedelta

from .base import get_customer_phone_number


def generate_support_event(customers, cell_sites, base_time=None, allow_data_issues=True, force_clean=False):
    """Generate customer support event."""
    support_channels = ["phone", "chat", "email", "store", "social_media", "ivr"]
    support_reasons = ["billing", "technical", "sales", "complaint", "information", "activation"]

    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time = base_time + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    phone_number = get_customer_phone_number(customers, allow_data_issues=allow_data_issues, force_clean=force_clean)

    channel = random.choice(support_channels)
    if channel == "phone":
        wait_time = random.randint(60, 1800)
        resolution_time = random.randint(300, 3600)
    elif channel == "chat":
        wait_time = random.randint(30, 600)
        resolution_time = random.randint(180, 1800)
    elif channel == "store":
        wait_time = random.randint(300, 3600)
        resolution_time = random.randint(600, 7200)
    else:
        wait_time = 0
        resolution_time = random.randint(60, 86400)

    event = {
        "event_type": "support",
        "sid": f"SU{random.randint(10**8, 10**9)}",
        "customer": phone_number,
        "channel": channel,
        "reason": random.choice(support_reasons),
        "wait_time_seconds": wait_time,
        "resolution_time_seconds": resolution_time,
        "agent_id": f"AG{random.randint(1000, 9999)}",
        "satisfaction_score": random.randint(1, 5) if random.random() < 0.7 else None,
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "phone_number": phone_number,
        "first_call_resolution": random.choice([True, False]),
        "escalated": random.random() < 0.2,
        "call_back_requested": random.random() < 0.3,
    }

    return event


