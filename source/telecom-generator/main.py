import argparse
import asyncio

from runners.batch_runner import run_batch, export_dataframe
from runners.stream_runner import run_stream


def parse_args():
    parser = argparse.ArgumentParser(description="Telecom event generator (modular).")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    batch_parser = subparsers.add_parser("batch", help="Generate batch dataset.")
    batch_parser.add_argument("--num-events", type=int, default=1000)
    batch_parser.add_argument("--days", type=int, default=30)
    batch_parser.add_argument("--clean-pct", type=int, default=70)
    batch_parser.add_argument("--output", type=str, default="telecom_events.csv")
    batch_parser.add_argument("--types", nargs="*", help="Allowed event types")

    stream_parser = subparsers.add_parser("stream", help="Simulate streaming.")
    stream_parser.add_argument("--rate", type=int, default=10, help="Events per minute")
    stream_parser.add_argument("--minutes", type=int, default=1, help="Duration in minutes")
    stream_parser.add_argument("--format", type=str, default="json", choices=["json", "csv", "kafka"])
    stream_parser.add_argument("--types", nargs="*", help="Allowed event types")

    return parser.parse_args()


def main():
    args = parse_args()

    if args.mode == "batch":
        df = run_batch(
            num_events=args.num_events,
            time_range_days=args.days,
            clean_row_percentage=args.clean_pct,
            allow_data_issues=True,
            allowed_event_types=args.types,
        )
        export_dataframe(df, output_file=args.output)
    elif args.mode == "stream":
        asyncio.run(
            run_stream(
                num_events_per_minute=args.rate,
                duration_minutes=args.minutes,
                output_format=args.format,
                allowed_event_types=args.types,
            )
        )


if __name__ == "__main__":
    main()





