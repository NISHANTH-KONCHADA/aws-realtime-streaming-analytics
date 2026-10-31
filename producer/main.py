"""
Producer Command-Line Interface (CLI).

Allows running the transaction generator locally (to a JSONL file),
dry-running Kinesis publishing, or publishing live records to an AWS Kinesis stream.

Usage:
    python -m producer.main --mode local --count 100 --output local_events.jsonl
    python -m producer.main --mode stream --rate 10 --dry-run
    python -m producer.main --mode stream --stream-name ecommerce-transaction-stream --region us-east-1
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from dotenv import load_dotenv

from producer.kinesis_producer import KinesisProducer
from producer.transaction_generator import TransactionGenerator

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)
logger = logging.getLogger("producer_main")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="E-Commerce Real-Time Event Producer and Simulator",
    )
    parser.add_argument(
        "--mode",
        choices=["local", "stream", "batch"],
        default="local",
        help="Execution mode: 'local' (file), 'stream' (continuous), or 'batch' (single batch)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Number of events to generate in 'local' or 'batch' mode",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="local_events.jsonl",
        help="Output path for local JSONL mode",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=5.0,
        help="Events per second in streaming mode",
    )
    parser.add_argument(
        "--stream-name",
        type=str,
        default=os.getenv("KINESIS_STREAM_NAME", "ecommerce-transaction-stream"),
        help="Target Amazon Kinesis Data Stream name",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=os.getenv("AWS_REGION", "us-east-1"),
        help="AWS Region name",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Deterministic random seed for repeatable testing",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate Kinesis publishing without making actual AWS API calls",
    )
    parser.add_argument(
        "--failure-rate",
        type=float,
        default=0.15,
        help="Proportion of simulated payment failures (0.0 to 1.0)",
    )
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.05,
        help="Proportion of duplicate events generated to test idempotency",
    )
    return parser.parse_args()


def run_local_mode(generator: TransactionGenerator, output_path: str, count: int) -> None:
    logger.info(f"Generating {count} events to local file: {output_path}")
    generator.write_to_jsonl(output_path, count=count)
    logger.info(f"Successfully wrote {count} events to {output_path}")


def run_stream_mode(
    generator: TransactionGenerator,
    producer: KinesisProducer,
    rate_per_sec: float,
    batch_size: int = 10,
) -> None:
    logger.info(
        f"Starting continuous streaming to '{producer.stream_name}' at {rate_per_sec} events/sec (Batch size: {batch_size})"
    )
    buffer = []
    try:
        for event in generator.stream_events(rate_per_sec=rate_per_sec):
            buffer.append(event)
            if len(buffer) >= batch_size:
                result = producer.put_records_batch(buffer)
                logger.info(
                    f"Published batch: {result['successful_count']} succeeded, {result['failed_count']} failed, {result['retries']} retries"
                )
                buffer.clear()
    except KeyboardInterrupt:
        if buffer:
            producer.put_records_batch(buffer)
        logger.info(
            f"Stream stopped. Final Producer Stats: {producer.stats}"
        )


def main() -> None:
    args = parse_args()

    generator = TransactionGenerator(
        seed=args.seed,
        failure_rate=args.failure_rate,
        duplicate_rate=args.duplicate_rate,
    )

    if args.mode == "local":
        run_local_mode(generator, args.output, args.count)
        return

    producer = KinesisProducer(
        stream_name=args.stream_name,
        region_name=args.region,
        dry_run=args.dry_run,
    )

    if args.mode == "batch":
        events = generator.generate_batch(args.count)
        result = producer.put_records_batch(events)
        logger.info(f"Batch result: {result}")
    elif args.mode == "stream":
        run_stream_mode(generator, producer, rate_per_sec=args.rate)


if __name__ == "__main__":
    main()
