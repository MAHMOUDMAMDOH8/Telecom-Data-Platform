import asyncio
import json
import random
import time
import signal
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka import KafkaProducer
from kafka.errors import KafkaError

from generators.base import load_customers, load_cell_sites
from generators.call import generate_call_event
from generators.sms import generate_sms_event
from generators.payment import generate_payment_event
from generators.recharge import generate_recharge_event
from generators.support import generate_support_event


# init topic for each event type
EVENT_TOPICS = {
    "call": "CALL_TOPIC",
    "sms": "SMS_TOPIC",
    "payment": "PAYMENT_TOPIC",
    "recharge": "RECHARGE_TOPIC",
    "support": "SUPPORT_TOPIC",
}

# Event generation weights (relative frequency)
EVENT_WEIGHTS = {
    "call": 30,
    "sms": 25,
    "payment": 15,
    "recharge": 20,
    "support": 10,
}

# Global variables
producer = None
customers = None
cell_sites = None
running = True


def signal_handler(sig, frame):
    """Handle graceful shutdown."""
    global running
    print("\nShutting down gracefully...")
    running = False
    if producer:
        producer.close()


def init_kafka_producer(bootstrap_servers='localhost:9092'):
    """Initialize Kafka producer."""
    try:
        return KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
        )
    except Exception as e:
        print(f"Error initializing Kafka producer: {e}")
        raise


def send_event_to_kafka(event_type, event_data):
    """Send event to Kafka topic."""
    global producer
    try:
        topic = EVENT_TOPICS.get(event_type)
        if not topic:
            print(f"Unknown event type: {event_type}")
            return False
        
        # Use customer phone number as key for partitioning
        key = event_data.get("customer") or event_data.get("phone_number") or event_data.get("from")
        
        future = producer.send(topic, value=event_data, key=key)
        # Optional: wait for acknowledgment (can be removed for better performance)
        # record_metadata = future.get(timeout=10)
        return True
    except KafkaError as e:
        print(f"Kafka error sending {event_type} event: {e}")
        return False
    except Exception as e:
        print(f"Error sending {event_type} event: {e}")
        return False


def generate_and_send_event(event_type, base_time=None):
    """Generate an event of the specified type and send it to Kafka."""
    global customers, cell_sites
    
    if not customers or not cell_sites:
        print("Error: Customers or cell sites not loaded")
        return False
    
    # Event generator mapping
    event_generators = {
        "call": generate_call_event,
        "sms": generate_sms_event,
        "payment": generate_payment_event,
        "recharge": generate_recharge_event,
        "support": generate_support_event,
    }
    
    try:
        # Get the appropriate generator function
        generator_func = event_generators.get(event_type)
        if not generator_func:
            print(f"Unknown event type: {event_type}")
            return False
        
        # Generate event
        event = generator_func(customers, cell_sites, base_time=base_time)
        
        # Send to Kafka
        success = send_event_to_kafka(event_type, event)
        if success:
            print(f"Sent {event_type} event: {event.get('sid', 'N/A')} at {event.get('timestamp', 'N/A')}")
        return success
    except Exception as e:
        print(f"Error generating {event_type} event: {e}")
        return False


def select_event_type():
    """Select event type based on weights."""
    event_types = list(EVENT_WEIGHTS.keys())
    weights = list(EVENT_WEIGHTS.values())
    return random.choices(event_types, weights=weights, k=1)[0]


async def stream_events(events_per_second=10, allow_data_issues=True):
    """Main streaming loop that generates and sends events."""
    global running, customers, cell_sites
    
    # Load data
    print("Loading customers and cell sites...")
    try:
        # Use correct filenames (DIM_USER.json is uppercase)
        customers = load_customers("DIM_USER.json")
        cell_sites = load_cell_sites("dim_cell_site.json")
        print(f"Loaded {len(customers)} customers and {len(cell_sites)} cell sites")
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    
    # Calculate delay between events
    delay = 1.0 / events_per_second if events_per_second > 0 else 1.0
    event_count = 0
    
    print(f"Starting event stream at {events_per_second} events/second...")
    print("Press Ctrl+C to stop")
    
    while running:
        try:
            # Select event type based on weights
            event_type = select_event_type()
            
            # Generate and send event
            generate_and_send_event(event_type)
            event_count += 1
            
            # Wait before next event
            await asyncio.sleep(delay)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error in streaming loop: {e}")
            await asyncio.sleep(1)
    
    print(f"\nStreaming stopped. Total events sent: {event_count}")


def run_stream(events_per_second=10, bootstrap_servers='localhost:9092', allow_data_issues=True):
    """Run the event stream."""
    global producer, running
    
    # Setup signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize Kafka producer
    print(f"Connecting to Kafka at {bootstrap_servers}...")
    try:
        producer = init_kafka_producer(bootstrap_servers)
        print("Kafka producer initialized successfully")
    except Exception as e:
        print(f"Failed to initialize Kafka producer: {e}")
        return
    
    # Run async event stream
    try:
        asyncio.run(stream_events(events_per_second, allow_data_issues))
    except KeyboardInterrupt:
        pass
    finally:
        if producer:
            producer.flush()
            producer.close()
        print("Kafka producer closed")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Stream telecom events to Kafka')
    parser.add_argument('--events-per-second', type=int, default=10,
                        help='Number of events to generate per second (default: 10)')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092',
                        help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--clean-data', action='store_true',
                        help='Generate clean data without quality issues')
    
    args = parser.parse_args()
    
    run_stream(
        events_per_second=args.events_per_second,
        bootstrap_servers=args.bootstrap_servers,
        allow_data_issues=not args.clean_data
    )
