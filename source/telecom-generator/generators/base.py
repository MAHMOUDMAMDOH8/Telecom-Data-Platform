"""
Shared utilities and constants for telecom event generation.
Extracted from the original monolithic generator for reuse.
"""

import random
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

# Core catalogs and constants
egyptian_cities = [
    {"city": "Cairo", "region": "Cairo", "lat_range": (29.9, 30.1), "lon_range": (31.2, 31.4)},
    {"city": "Alexandria", "region": "Alexandria", "lat_range": (31.0, 31.3), "lon_range": (29.8, 30.0)},
    {"city": "Giza", "region": "Giza", "lat_range": (29.9, 30.1), "lon_range": (31.1, 31.3)},
]

BEHAVIOR_PROFILES = {
    "heavy_streamer": {
        "weights": {"sms": 20, "call": 25, "data_usage": 40, "recharge": 5, "payment": 5, "wifi_usage": 3, "usage": 2},
        "data_multiplier": 3.0,
        "call_multiplier": 0.5,
        "typical_apps": ["YouTube", "Netflix", "Spotify", "TikTok", "Twitch"],
    },
    "business_user": {
        "weights": {"sms": 20, "call": 50, "data_usage": 15, "recharge": 5, "payment": 5, "wifi_usage": 3, "usage": 2},
        "call_multiplier": 2.5,
        "sms_multiplier": 1.5,
        "international_multiplier": 3.0,
        "typical_hours": [8, 9, 10, 11, 14, 15, 16, 17],
    },
    "light_user": {
        "weights": {"sms": 40, "call": 30, "data_usage": 10, "recharge": 10, "payment": 5, "wifi_usage": 3, "usage": 2},
        "all_multiplier": 0.3,
        "typical_days": ["Saturday", "Sunday"],
    },
    "international": {
        "weights": {"sms": 15, "call": 40, "data_usage": 20, "recharge": 10, "payment": 5, "wifi_usage": 5, "usage": 5},
        "roaming_weight": 15,
        "international_multiplier": 5.0,
    },
    "gamer": {
        "weights": {"sms": 10, "call": 10, "data_usage": 60, "recharge": 10, "payment": 5, "wifi_usage": 3, "usage": 2},
        "data_multiplier": 4.0,
        "latency_sensitive": True,
        "typical_apps": ["Steam", "Discord", "Twitch", "PUBG", "Fortnite"],
    },
    "social_media": {
        "weights": {"sms": 30, "call": 20, "data_usage": 35, "recharge": 5, "payment": 5, "wifi_usage": 3, "usage": 2},
        "sms_multiplier": 2.0,
        "data_multiplier": 2.5,
        "typical_apps": ["Facebook", "Instagram", "WhatsApp", "Twitter", "Snapchat"],
    },
}

FRAUD_PATTERNS = {
    "international_short_calls": {"marker": "WANGIRI_FRAUD"},
    "midnight_data_spike": {"marker": "BOTNET_ACTIVITY"},
    "velocity_anomaly": {"marker": "CLONED_SIM"},
    "roaming_activation_fraud": {"marker": "ROAMING_FRAUD"},
}

ROAMING_COUNTRIES = [
    {"country": "USA", "operator": "AT&T", "rate_per_min": 2.5, "rate_per_mb": 0.15},
    {"country": "UK", "operator": "Vodafone", "rate_per_min": 1.8, "rate_per_mb": 0.12},
    {"country": "UAE", "operator": "Etisalat", "rate_per_min": 1.2, "rate_per_mb": 0.08},
]

SECURITY_EVENTS = [
    {"type": "failed_login", "severity": "medium", "typical_count": 3},
    {"type": "sim_swap_request", "severity": "high", "typical_count": 1},
    {"type": "number_porting_request", "severity": "high", "typical_count": 1},
]

sample_messages = [
    "Hello!",
    "Your verification code is 123456",
    "Service outage in your area",
    "Network maintenance scheduled",
]

tac_to_manufacturer = {
    "35846279": "Samsung",
    "35846270": "Apple",
    "86945303": "Huawei",
    "35846103": "Xiaomi",
    "35944803": "Oppo",
}
tac_codes = list(tac_to_manufacturer.keys())


# Helpers
def calculate_distance(loc1: dict, loc2: dict) -> float:
    from math import radians, sin, cos, sqrt, atan2

    R = 6371
    lat1, lon1 = radians(loc1.get("lat", 0)), radians(loc1.get("lon", 0))
    lat2, lon2 = radians(loc2.get("lat", 0)), radians(loc2.get("lon", 0))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def apply_seasonal_patterns(timestamp: datetime) -> float:
    hour = timestamp.hour
    day = timestamp.weekday()
    month = timestamp.month
    multiplier = 1.0
    if 8 <= hour <= 18:
        multiplier *= 1.3
    elif 19 <= hour <= 23:
        multiplier *= 1.5
    elif 0 <= hour <= 6:
        multiplier *= 0.3
    if day >= 5:
        multiplier *= 1.4
    if month == 9:
        multiplier *= 1.6
        if 20 <= hour <= 24 or 0 <= hour <= 3:
            multiplier *= 1.5
    if 6 <= month <= 8:
        multiplier *= 1.2
    return multiplier


def get_customer_behavior_profile(customer_id: str) -> str:
    profiles = list(BEHAVIOR_PROFILES.keys())
    return profiles[hash(customer_id) % len(profiles)]


def generate_location_data(city_info: dict = None) -> dict:
    if not city_info:
        city_info = random.choice(egyptian_cities)
    lat = random.uniform(*city_info["lat_range"])
    lon = random.uniform(*city_info["lon_range"])
    lat += random.uniform(-0.01, 0.01)
    lon += random.uniform(-0.01, 0.01)
    return {
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "city": city_info["city"],
        "region": city_info["region"],
        "accuracy_meters": random.randint(10, 500),
        "location_source": random.choice(["GPS", "CELL", "WIFI", "IP"]),
        "speed_kph": round(random.uniform(0, 120), 1),
    }


def generate_network_metrics() -> dict:
    return {
        "signal_strength_dbm": random.randint(-120, -50),
        "latency_ms": random.randint(10, 500),
        "packet_loss_percent": round(random.uniform(0, 5), 2),
        "cell_congestion": random.choice(["low", "medium", "high"]),
        "handover_success": random.choice([True, False]),
        "mos_score": round(random.uniform(1.0, 5.0), 2),
        "jitter_ms": round(random.uniform(0, 50), 2),
        "throughput_mbps": round(random.uniform(1, 100), 2),
    }


def load_customers(filepath="dim_user.json"):
    filepath = Path(filepath)
    if not filepath.is_absolute():
        filepath = Path(__file__).resolve().parents[2] / filepath
    with filepath.open("r", encoding="utf-8") as f:
        users = json.load(f)
        for idx, user in enumerate(users, start=1001):
            user["customer_id"] = user["phone_number"]
            user["number"] = user["phone_number"]
            user.setdefault("plan_type", random.choice(["Prepaid", "Postpaid"]))
            user.setdefault("status", random.choice(["Active", "Inactive", "Suspended"]))
            user["behavior_profile"] = get_customer_behavior_profile(user["customer_id"])
            city_info = random.choice(egyptian_cities)
            location = generate_location_data(city_info)
            user.update(location)
        return users


def load_cell_sites(filepath="dim_cell_site.json"):
    filepath = Path(filepath)
    if not filepath.is_absolute():
        filepath = Path(__file__).resolve().parents[2] / filepath
    with filepath.open("r", encoding="utf-8") as f:
        return json.load(f)


def generate_imei_with_manufacturer(allow_invalid=False):
    if allow_invalid and random.random() < 0.05:
        if random.random() < 0.5:
            return "".join([str(random.randint(0, 9)) for _ in range(10)]), None
        return "".join([random.choice("0123456789ABCDEF") for _ in range(15)]), None
    tac = random.choice(tac_codes)
    serial = "".join([str(random.randint(0, 9)) for _ in range(7)])
    imei = tac + serial
    return imei, tac_to_manufacturer[tac]


def generate_user_info(customers, cell_sites, allow_data_issues=False, force_clean=False):
    customer = random.choice(customers)
    imei, manufacturer = generate_imei_with_manufacturer(allow_invalid=(allow_data_issues and not force_clean))
    phone_number = customer["phone_number"]
    if allow_data_issues and not force_clean:
        if random.random() < 0.06:
            issue_type = random.choice(["missing", "invalid", "malformed", "extra_chars"])
            if issue_type == "missing":
                phone_number = None
            elif issue_type == "invalid":
                phone_number = "0000000000"
            elif issue_type == "malformed":
                phone_number = f"+20{random.randint(100000000, 999999999)}"
            elif issue_type == "extra_chars":
                phone_number = f"  {phone_number}  "

    cell_site_id = random.choice(cell_sites)["cell_id"]
    if allow_data_issues and not force_clean and random.random() < 0.04:
        if random.random() < 0.5:
            cell_site_id = None
        else:
            cell_site_id = "INVALID_CELL"

    location_data = {
        "city": customer.get("city"),
        "region": customer.get("region"),
        "latitude": customer.get("latitude"),
        "longitude": customer.get("longitude"),
    }
    network_metrics = generate_network_metrics() if random.random() < 0.7 else None
    return {
        "number": phone_number,
        "cell_site": cell_site_id,
        "imei": imei,
        "customer_id": customer["customer_id"],
        "plan_type": customer["plan_type"],
        "behavior_profile": customer.get("behavior_profile"),
        "location": location_data,
        "network_metrics": network_metrics,
        "typical_apps": customer.get("typical_apps", []),
        "manufacturer": manufacturer,
    }


def get_customer_phone_number(customers, allow_data_issues=False, force_clean=False):
    """Get just the phone number from a random customer (for simplified customer field)."""
    customer = random.choice(customers)
    phone_number = customer["phone_number"]
    
    if allow_data_issues and not force_clean:
        if random.random() < 0.06:
            issue_type = random.choice(["missing", "invalid", "malformed", "extra_chars"])
            if issue_type == "missing":
                phone_number = None
            elif issue_type == "invalid":
                phone_number = "0000000000"
            elif issue_type == "malformed":
                phone_number = f"+20{random.randint(100000000, 999999999)}"
            elif issue_type == "extra_chars":
                phone_number = f"  {phone_number}  "
    
    return phone_number


def introduce_data_quality_issues(event, event_type, force_clean=False):
    if force_clean:
        return []
    issues_applied = []
    if random.random() < 0.08:
        if event_type == "sms":
            if random.random() < 0.5:
                event["body"] = None
            else:
                event["from"] = None
        issues_applied.append("null_field")
    if random.random() < 0.03:
        if random.random() < 0.5:
            event["timestamp"] = "INVALID_DATE"
        else:
            event["timestamp"] = "2025-13-45 25:99:99"
        issues_applied.append("invalid_timestamp")
    if random.random() < 0.02:
        event["sid"] = None
        issues_applied.append("missing_sid")
    return issues_applied


def safe_parse_timestamp(timestamp_str, default_time=None):
    if timestamp_str is None or not isinstance(timestamp_str, str):
        return default_time or datetime.now()
    try:
        return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return default_time or datetime.now()


