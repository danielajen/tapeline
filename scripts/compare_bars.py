#!/usr/bin/env python3
"""Compares bars produced by the live path against bars produced by replay.

The comparison is deliberately not "the two sets are identical", because that
would be the wrong assertion and it would fail for a reason that is not a
defect. The live job runs against an unbounded source and is cancelled, so its
final windows never close and never emit. The replay runs against a bounded
source and closes everything. The honest question is whether the windows both
produced agree, and whether enough of them overlap for that agreement to mean
something.

A bar is keyed on (venue, symbol, window_start_us). Values are compared with a
tolerance on the floating point fields, because VWAP is a sum of products
divided by a sum, and the two paths can accumulate in a different order.
Ordering changes the last bits of a double; it must not change anything else.

    compare_bars.py live.jsonl replay.jsonl
"""
import json
import sys

# Enough to catch a real aggregation difference, loose enough to permit
# summation order. A VWAP that differs in the seventh significant figure is
# floating point; one that differs in the third is a bug.
REL_TOL = 1e-9

FIELDS = ("open", "high", "low", "close", "volume", "vwap", "trade_count")


def load(path):
    bars = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                b = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = (b.get("venue"), b.get("symbol"), b.get("window_start_us"))
            bars[key] = b
    return bars


def close(a, b):
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if a == b:
            return True
        scale = max(abs(a), abs(b), 1e-12)
        return abs(a - b) / scale <= REL_TOL
    return a == b


def main():
    live = load(sys.argv[1])
    replay = load(sys.argv[2])

    shared = sorted(set(live) & set(replay))
    mismatches = []

    for key in shared:
        for field in FIELDS:
            lv, rv = live[key].get(field), replay[key].get(field)
            if not close(lv, rv):
                mismatches.append((key, field, lv, rv))

    print(f"""
Kappa backfill: replayed bars vs live bars
==========================================
live bars                 {len(live)}
replayed bars             {len(replay)}
windows in both           {len(shared)}
mismatched fields         {len(mismatches)}
""")

    for key, field, lv, rv in mismatches[:20]:
        print(f"  {key} {field}: live={lv} replay={rv}")

    if not shared:
        print("::error::no window appears in both outputs; nothing was compared")
        return 1

    # A handful of overlapping windows could agree by luck. The seed produces
    # tens of windows per key, so requiring a floor here is what makes the
    # result evidence rather than an anecdote.
    if len(shared) < 5:
        print(f"::error::only {len(shared)} overlapping windows; too few to be evidence")
        return 1

    if mismatches:
        print(f"::error::{len(mismatches)} field(s) differ between live and replayed bars")
        return 1

    print(f"OK: {len(shared)} windows agree on every field across both paths.")
    print("""
Windows present in only one output are expected and are not a defect: the live
job is cancelled against an unbounded source so its final windows never close,
while the bounded replay closes all of them. What would be a defect is any
window computed by both and answered differently.
""")
    return 0


if __name__ == "__main__":
    sys.exit(main())
