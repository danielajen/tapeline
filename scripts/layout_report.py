#!/usr/bin/env python3
"""Turns three checkpoint_stats.py outputs into the comparison table.

The per-topic layout is two jobs, so its numbers are not a single reading.
Which way they combine matters and is not obvious, so it is stated here rather
than left implicit in an arithmetic expression:

  - Checkpoint duration takes the WORST of the two jobs. A pipeline is as slow
    to checkpoint as its slowest independent job, and using an average would
    flatter the split layout by hiding the book job behind the trade job.
  - Failed checkpoints are SUMMED, because every failure is a real failure
    somewhere.
  - State size is SUMMED, because that is the total being persisted.

The comparison that actually matters is not any single number but blast
radius, which needs no measurement: a monolith restarts every stage on any
failure, and the split layout restarts one.

    layout_report.py monolith.json book.json trades.json
"""
import json
import sys


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def main():
    mono, book, trades = (load(p) for p in sys.argv[1:4])

    if not mono or not book or not trades:
        print("::error::a layout produced no statistics; cannot compare")
        return 1

    if mono["completed"] < 1 or book["completed"] < 1 or trades["completed"] < 1:
        print("::error::a layout completed no checkpoints; the comparison is meaningless")
        print(json.dumps({"monolith": mono, "book": book, "trades": trades}, indent=2))
        return 1

    split_dur_avg = max(book["duration_avg_ms"], trades["duration_avg_ms"])
    split_dur_max = max(book["duration_max_ms"], trades["duration_max_ms"])
    split_failed = book["failed"] + trades["failed"]
    split_state = book["state_size_avg_b"] + trades["state_size_avg_b"]

    print(f"""
Job layout: monolith vs per-topic
=================================
Identical seeded input, same cluster, {mono['completed']}+ checkpoints each.

                              monolith      per-topic
jobs                          1             2
stages in one failure domain  {mono['stages']:<13} {max(book['stages'], trades['stages'])}
checkpoints completed         {mono['completed']:<13} {book['completed']} (book) / {trades['completed']} (trades)
checkpoints failed            {mono['failed']:<13} {split_failed}
checkpoint duration avg (ms)  {mono['duration_avg_ms']:<13.0f} {split_dur_avg:.0f}
checkpoint duration max (ms)  {mono['duration_max_ms']:<13.0f} {split_dur_max:.0f}
state persisted avg (B)       {mono['state_size_avg_b']:<13.0f} {split_state:.0f}

Restart blast radius is the finding that needs no statistics: a failure in any
stage of the monolith restarts all of them, including order book state that
had nothing to do with the fault. In the split layout it restarts one job.

Not measured here: max sustainable throughput and steady-state CPU. Both need
sustained load on dedicated hardware; a 4-vCPU shared runner over 90 seconds
cannot produce an honest number for either, and inventing one would be worse
than leaving the row empty.
""")

    with open("job-layout-machine.txt", "w") as f:
        f.write(f"monolith_duration_avg_ms={mono['duration_avg_ms']:.0f}\n")
        f.write(f"split_duration_avg_ms={split_dur_avg:.0f}\n")
        f.write(f"monolith_failed={mono['failed']}\nsplit_failed={split_failed}\n")
        f.write(f"monolith_stages={mono['stages']}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
