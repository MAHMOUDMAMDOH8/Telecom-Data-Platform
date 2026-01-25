import random
from datetime import datetime, timedelta

from .base import apply_seasonal_patterns, get_customer_phone_number, introduce_data_quality_issues


def generate_payment_event(customers, cell_sites, base_time=None, allow_data_issues=True, force_clean=False):
    """Generate payment event (bill payment, subscription payment)."""
    payment_status = random.choice(["success", "pending", "failed", "refunded"])
    payment_types = ["Bill Payment", "Subscription", "Plan Upgrade", "Service Fee", "Late Fee"]
    payment_type = random.choice(payment_types)

    if base_time is None:
        base_time = datetime.now() - timedelta(days=random.randint(0, 30))
        base_time = base_time + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))

    seasonal_mult = apply_seasonal_patterns(base_time)

    if payment_type == "Bill Payment":
        payment_amount = round(random.uniform(50, 500) * seasonal_mult, 2)
    elif payment_type == "Subscription":
        payment_amount = round(random.uniform(100, 300) * seasonal_mult, 2)
    elif payment_type == "Plan Upgrade":
        payment_amount = round(random.uniform(200, 1000) * seasonal_mult, 2)
    else:
        payment_amount = round(random.uniform(10, 100) * seasonal_mult, 2)

    payment_methods = ["Credit Card", "Debit Card", "Mobile Wallet", "Bank Transfer", "Auto-Debit"]
    payment_method = random.choice(payment_methods)
    phone_number = get_customer_phone_number(customers, allow_data_issues=allow_data_issues, force_clean=force_clean)

    event = {
        "event_type": "payment",
        "sid": f"PAY{random.randint(10**8, 10**9)}",
        "customer": phone_number,
        "payment_type": payment_type,
        "payment_amount": payment_amount,
        "payment_method": payment_method,
        "status": payment_status,
        "timestamp": base_time.strftime("%Y-%m-%d %H:%M:%S"),
        "phone_number": phone_number,
        "transaction_id": f"PAY{random.randint(10**10, 10**11)}",
        "invoice_number": f"INV{random.randint(10**6, 10**7)}" if payment_type == "Bill Payment" else None,
        "seasonal_multiplier": round(seasonal_mult, 2),
        "billing_info": {"amount": payment_amount, "currency": "EGP"},
    }

    if allow_data_issues:
        introduce_data_quality_issues(event, "payment", force_clean=force_clean)
        if not force_clean and random.random() < 0.04:
            event["payment_amount"] = None if random.random() < 0.5 else event.get("payment_amount")
            event["payment_method"] = None if event["payment_amount"] is not None else event.get("payment_method")

    if payment_status == "failed" and random.random() < 0.6:
        event["_requires_retry"] = True

    return event


