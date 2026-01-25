import random
from datetime import datetime, timedelta

from .base import apply_seasonal_patterns, get_customer_phone_number, introduce_data_quality_issues


def generate_recharge_event(customers, cell_sites, base_time=None, allow_data_issues=True, force_clean=False):
    """Generate recharge/top-up event."""
    recharge_status = random.choice(["success", "pending", "failed", "processing"])

    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time = base_time + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    seasonal_mult = apply_seasonal_patterns(base_time)
    recharge_amounts = [10, 20, 50, 100, 200, 500, 1000]
    recharge_amount = random.choice(recharge_amounts)
    payment_methods = ["Credit Card", "Debit Card", "Mobile Wallet", "Bank Transfer", "Cash", "Online"]
    payment_method = random.choice(payment_methods)

    phone_number = get_customer_phone_number(customers, allow_data_issues=allow_data_issues, force_clean=force_clean)

    if recharge_status == "success":
        balance_after = round(random.uniform(recharge_amount, recharge_amount + 500) * seasonal_mult, 2)
    else:
        balance_after = round(random.uniform(0, 100), 2)

    event = {
        "event_type": "recharge",
        "sid": f"RC{random.randint(10**8, 10**9)}",
        "customer": phone_number,
        "recharge_amount": recharge_amount,
        "balance_before": round(random.uniform(0, 50), 2),
        "balance_after": balance_after,
        "payment_method": payment_method,
        "status": recharge_status,
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "phone_number": phone_number,
        "transaction_id": f"TXN{random.randint(10**10, 10**11)}",
        "seasonal_multiplier": round(seasonal_mult, 2),
        "billing_info": {"amount": recharge_amount, "currency": "EGP"},
    }

    if allow_data_issues:
        introduce_data_quality_issues(event, "recharge", force_clean=force_clean)
        if not force_clean and random.random() < 0.04:
            event["recharge_amount"] = None if random.random() < 0.5 else -50

    if recharge_status == "failed" and random.random() < 0.7:
        event["_requires_followup"] = True

    return event


