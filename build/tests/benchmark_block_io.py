#!/usr/bin/env python3
"""Compare two block_io_bench release binaries with alternating run order."""

import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("/tmp"))
    parser.add_argument("--cpus", help="optional taskset CPU list")
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--chunks", default="131072,1048576")
    parser.add_argument("--sendfile", default="1", choices=["0", "1"])
    parser.add_argument("--separate", action="store_true", help="group writes before reads")
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("--rounds must be positive")
    chunks = [int(value) for value in args.chunks.split(",")]
    if any(value <= 0 or value > 16 * 1024 * 1024 for value in chunks):
        parser.error("chunk sizes must be within 1..16MiB")
    args.output.mkdir(parents=True, exist_ok=False)
    versions = {"before": args.before.resolve(), "after": args.after.resolve()}
    metadata = {
        "binaries": {name: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                     for name, path in versions.items()},
        "rounds": args.rounds, "chunks": chunks, "cpus": args.cpus,
        "data_dir": str(args.data_dir.resolve()), "sendfile": args.sendfile,
        "separate": args.separate,
    }
    (args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    grouped = {}
    for repeat in range(args.rounds):
        order = ["before", "after"] if repeat % 2 == 0 else ["after", "before"]
        for chunk in chunks:
            for version in order:
                name = f"{version}-chunk{chunk}-run{repeat}"
                command = [str(versions[version]), str(args.data_dir.resolve()), args.sendfile, str(chunk)]
                command.append("1" if args.separate else "0")
                if args.cpus:
                    command = ["taskset", "-c", args.cpus, *command]
                csv_path = args.output / f"{name}.csv"
                with csv_path.open("w") as output, (args.output / f"{name}.log").open("w") as log:
                    subprocess.run(command, stdout=output, stderr=log, check=True, timeout=180)
                with csv_path.open() as result:
                    rows = list(csv.DictReader(result))
                if len(rows) != 6 or any(int(row["chunk_bytes"]) != chunk for row in rows):
                    raise ValueError(f"Unexpected benchmark output: {csv_path}")
                for row in rows:
                    key = (version, chunk, row["direction"], int(row["block_bytes"]))
                    grouped.setdefault(key, []).append(row)
                print(name, flush=True)
    summary = []
    for (version, chunk, direction, size), rows in grouped.items():
        summary.append({
            "version": version, "chunk_bytes": chunk, "direction": direction, "block_bytes": size,
            **{key: statistics.median(float(row[key]) for row in rows)
               for key in ["mean_us", "p50_us", "p99_us", "mib_per_s"]},
        })
    summary.sort(key=lambda row: (row["chunk_bytes"], row["block_bytes"], row["direction"], row["version"]))
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    with (args.output / "summary.csv").open("w") as output:
        writer = csv.DictWriter(output, fieldnames=summary[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
