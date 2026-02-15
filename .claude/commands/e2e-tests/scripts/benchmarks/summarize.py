#!/usr/bin/env python3
import json
import os
import sys


def load(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def mean_ms(j: dict) -> float:
    return j["results"][0]["mean"] * 1000.0


def std_ms(j: dict) -> float:
    return j["results"][0]["stddev"] * 1000.0


def main(root: str) -> None:
    pairs = [
        ("vger backup", "vger.backup.json"),
        ("vger restore", "vger.restore.json"),
        ("restic backup", "restic.backup.json"),
        ("restic restore", "restic.restore.json"),
        ("rustic backup", "rustic.backup.json"),
        ("rustic restore", "rustic.restore.json"),
    ]
    print(f"root: {root}")
    for name, fn in pairs:
        p = os.path.join(root, fn)
        j = load(p)
        print(f"{name:14s} mean={mean_ms(j):9.1f} ms  std={std_ms(j):8.1f} ms")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <benchmark_out_root>", file=sys.stderr)
        raise SystemExit(2)
    main(sys.argv[1])

