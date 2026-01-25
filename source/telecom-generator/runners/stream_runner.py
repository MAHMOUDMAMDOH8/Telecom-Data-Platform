import asyncio
import json
import random
import time
from datetime import datetime, timedelta

from generators.base import load_customers, load_cell_sites
from generators.call import generate_call_event
from generators.sms import generate_sms_event
from generators.payment import generate_payment_event
from generators.recharge import generate_recharge_event
from generators.support import generate_support_event




async def run_stream(num_events_per_minute=10, duration_minutes=60, output_format="json", allowed_event_types=None):
    """
    Simulate real-time data streaming for a subset of event types.
    """
    print(f"Starting real-time stream: {num_events_per_minute} events/minute for {duration_minutes} minutes")

    base_path = __file__
    customers = load_customers()
    cell_sites = load_cell_sites()

    event_generators = {
        "sms": generate_sms_event,
        "call": generate_call_event,
        "payment": generate_payment_event,
        "recharge": generate_recharge_event,
        "support": generate_support_event,
    }

    event_weights = {"sms": 25, "call": 25, "payment": 20, "recharge": 5, "support": 2}

    if allowed_event_types:
        event_generators = {k: v for k, v in event_generators.items() if k in allowed_event_types}
        event_weights = {k: v for k, v in event_weights.items() if k in allowed_event_types}
        if not event_generators:
            raise ValueError("No event generators available after filtering allowed_event_types.")

    event_types = list(event_weights.keys())
    weights = list(event_weights.values())

    total_events = num_events_per_minute * duration_minutes
    events_generated = 0

    for minute in range(duration_minutes):
        minute_start = time.time()
        print(f"\nMinute {minute + 1}/{duration_minutes}: ", end="")

        for second in range(60):
            events_this_second = num_events_per_minute // 60
            if second < num_events_per_minute % 60:
                events_this_second += 1

            for _ in range(events_this_second):
                event_time = datetime.now() - timedelta(seconds=random.uniform(0, 1))
                event_type = random.choices(event_types, weights=weights)[0]
                event = event_generators[event_type](
                    customers, cell_sites, base_time=event_time, allow_data_issues=True, force_clean=random.random() < 0.8
                )
                events_generated += 1

                if output_format == "json":
                    print(json.dumps(event))
                elif output_format == "csv":
                    flat_event = flatten_event(event)
                    print(",".join(str(v) for v in flat_event.values()))
                elif output_format == "kafka":
                    kafka_msg = {
                        "topic": "telecom-events",
                        "partition": random.randint(0, 3),
                        "offset": events_generated,
                        "timestamp": int(time.time() * 1000),
                        "value": event,
                    }
                    print(f"KAFKA: {json.dumps(kafka_msg)}")

                await asyncio.sleep(0.001)

        elapsed = time.time() - minute_start
        if elapsed < 60:
            await asyncio.sleep(60 - elapsed)

    print(f"\nStreaming completed. Total events generated: {events_generated}")


def flatten_event(event):
    flat = {}
    for key, value in event.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat[f"{key}_{sub_key}"] = sub_value
        elif isinstance(value, list):
            flat[key] = json.dumps(value)
        else:
            flat[key] = value
    return flat


if __name__ == "__main__":
    asyncio.run(run_stream())





