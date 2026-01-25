"""
Build DIM_USER and DIM_DEVICE dimension tables from the raw source JSON files.

Inputs:
  - dim_user.json              (existing user master; contains phone_number)
  - tac_to_manufacturer.json   (TAC master; tac + manufacturer)

Outputs (CSV):
  - DIM_USER.csv
  - DIM_DEVICE.csv
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

ROOT = Path(__file__).resolve().parent
USER_SOURCE = ROOT.parent / "dim_user.json"
DEVICE_SOURCE = ROOT.parent / "tac_to_manufacturer.json"
DIM_USER_OUT = ROOT / "DIM_USER.json"
DIM_DEVICE_OUT = ROOT / "DIM_DEVICE.json"


def _safe_date(value: str | None, fallback: datetime) -> datetime:
    if not value or not isinstance(value, str):
        return fallback
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return fallback


def build_dim_user():
    with USER_SOURCE.open("r", encoding="utf-8") as f:
        users: List[Dict[str, Any]] = json.load(f)

    records = []
    now = datetime.now()
    for idx, user in enumerate(users, start=1):
        msisdn = user.get("phone_number") or user.get("msisdn")
        activation_date_raw = user.get("activation_date") or user.get("registration_date")
        activation_date = _safe_date(activation_date_raw, now - timedelta(days=random.randint(30, 365)))

        # Optional attributes
        gender = user.get("gender") or random.choice(["Male", "Female"])
        age = user.get("age")
        age_group = user.get("age_group")
        if not age_group:
            if isinstance(age, int):
                if age < 18:
                    age_group = "<18"
                elif age < 30:
                    age_group = "18-29"
                elif age < 45:
                    age_group = "30-44"
                elif age < 60:
                    age_group = "45-59"
                else:
                    age_group = "60+"
            else:
                age_group = "unknown"

        customer_type = user.get("plan_type") or user.get("customer_type") or "Prepaid"
        status = user.get("status") or "Active"
        city = user.get("city") or user.get("region") or "Unknown"

        effective_from = activation_date
        effective_to = datetime(9999, 12, 31)

        records.append(
            {
                "user_sk": idx,
                "msisdn": msisdn,
                "customer_type": customer_type,
                "gender": gender,
                "age_group": age_group,
                "city": city,
                "activation_date": activation_date.date(),
                "status": status,
                "effective_from": effective_from.date(),
                "effective_to": effective_to.date(),
                "is_current": True,
            }
        )

    df = pd.DataFrame(records)
    df["activation_date"] = df["activation_date"].astype(str)
    df["effective_from"] = df["effective_from"].astype(str)
    df["effective_to"] = df["effective_to"].astype(str)
    df.to_json(DIM_USER_OUT, orient="records", force_ascii=False)
    print(f"Wrote {len(df)} rows to {DIM_USER_OUT}")


def build_dim_device():
    with DEVICE_SOURCE.open("r", encoding="utf-8") as f:
        devices: List[Dict[str, Any]] = json.load(f)

    seen_tac = set()
    records = []
    for idx, dev in enumerate(devices, start=1):
        tac = str(dev.get("tac")).strip()
        if not tac or tac in seen_tac:
            continue
        seen_tac.add(tac)
        brand = dev.get("manufacturer") or dev.get("brand") or "Unknown"
        # Placeholder enrichments
        model = dev.get("model") or "Generic"
        os_name = dev.get("os") or "Unknown"
        is_smartphone = dev.get("is_smartphone")
        if is_smartphone is None:
            is_smartphone = True
        imei_stub = tac + "0000000"

        records.append(
            {
                "device_sk": idx,
                "imei": imei_stub,
                "tac": tac,
                "brand": brand,
                "model": model,
                "os": os_name,
                "is_smartphone": bool(is_smartphone),
            }
        )

    df = pd.DataFrame(records)
    df.to_json(DIM_DEVICE_OUT, orient="records", force_ascii=False)
    print(f"Wrote {len(df)} rows to {DIM_DEVICE_OUT}")


def main():
    build_dim_user()
    build_dim_device()


if __name__ == "__main__":
    main()

