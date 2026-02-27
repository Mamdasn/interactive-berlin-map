import argparse
import logging
from datetime import date, datetime, timedelta

from libs.locations import build_locations


def parse_start_date(value: str) -> date:
    if value == "today":
        return date.today()

    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--from must be 'today' or YYYY-MM-DD"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Warm locations and geocode caches for a date range."
    )
    parser.add_argument(
        "--from",
        dest="start_date",
        type=parse_start_date,
        default="today",
        help="Start date: 'today' or YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of consecutive days to warm (default: 30)",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.days < 1:
        parser.error("--days must be at least 1")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    for offset in range(args.days):
        target_date = args.start_date + timedelta(days=offset)
        locations = build_locations(target_date)
        logging.info(
            "Warmed caches for date=%s locations=%d",
            target_date.isoformat(),
            len(locations),
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
