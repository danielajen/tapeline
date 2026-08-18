#!/usr/bin/env python3
"""Reads one job's checkpoint statistics from the Flink REST API.

Written as a script rather than inline in a workflow because the job layout
comparison needs the identical query run against three jobs, and a
copy-pasted inline version would be three chances to measure the layouts
differently.

Every value comes from Flink's own accounting. Nothing here is derived from
wall-clock timing in the workflow, which on a shared runner would measure the
runner.

    checkpoint_stats.py <job-id> <label>
"""
import json
import sys
import urllib.request

REST = "http://localhost:8090"


def fetch(path):
    with urllib.request.urlopen(f"{REST}{path}", timeout=15) as r:
        return json.load(r)


def main():
    if len(sys.argv) != 3:
        print("usage: checkpoint_stats.py <job-id> <label>", file=sys.stderr)
        return 2

    jid, label = sys.argv[1], sys.argv[2]
    job = fetch(f"/jobs/{jid}")
    cps = fetch(f"/jobs/{jid}/checkpoints")

    counts = cps.get("counts", {})
    summary = cps.get("summary", {})

    def stat(group, key, default=0):
        return (summary.get(group) or {}).get(key) or default

    # read-records on a source vertex is 0 by construction, and write-records
    # on a vertex chained with a Kafka committer is 0 however much it emits.
    # Summing them produces numbers that look meaningful and are not; this
    # repo already shipped that mistake once. Report per vertex instead.
    vertices = [
        {
            "name": v["name"][:60],
            "in": v["metrics"]["read-records"],
            "out": v["metrics"]["write-records"],
        }
        for v in job.get("vertices", [])
    ]

    out = {
        "label": label,
        "state": job.get("state"),
        "stages": len(vertices),
        "completed": counts.get("completed", 0),
        "failed": counts.get("failed", 0),
        "duration_avg_ms": stat("end_to_end_duration", "avg"),
        "duration_max_ms": stat("end_to_end_duration", "max"),
        "state_size_avg_b": stat("state_size", "avg"),
        "state_size_max_b": stat("state_size", "max"),
        "vertices": vertices,
    }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
