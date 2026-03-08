"""Argument parsing and entry point."""

from __future__ import annotations

import argparse
import sys

from .defaults import ConfigError, TOOLS, build_config
from .runner import run_benchmarks


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="benchmark-runner",
        description="Reproducible benchmark harness for vykar vs restic vs rustic vs borg vs kopia.",
    )
    parser.add_argument("--runs", type=int, required=True, help="timed runs per operation")
    parser.add_argument(
        "--tool",
        choices=TOOLS,
        default=None,
        help="limit to a single tool",
    )
    parser.add_argument(
        "--dataset",
        default=None,
        help="dataset directory (default: ~/corpus-local)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = build_config(runs=args.runs, tool=args.tool, dataset=args.dataset)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return run_benchmarks(config)


if __name__ == "__main__":
    raise SystemExit(main())
