#!/usr/bin/env python3
"""Measures time from opening a quote stream to receiving the first quote.

Why this is not measured with k6, which already drives this endpoint under
load: k6 reported a first-quote p95 of ~30,000 ms against a 30-second hold, on
two separate runs, and slicing the hold into 50 ms pieces did not move it. The
value reproduced the hold duration to four significant figures, which is the
signature of a harness timing itself rather than the server. That measurement
was removed rather than kept as a threshold nobody could interpret.

This probe sidesteps the question entirely instead of guessing at k6's event
loop. It opens one real stream per sample with grpcurl, timestamps the first
JSON object that arrives on stdout, and closes. No event loop, no VU
scheduler, nothing between the socket and the clock but a pipe.

Each sample is an independent process and connection, so this measures cold
time-to-first-quote: dial, authenticate, subscribe, snapshot. That is the
number a client actually experiences on connect, and it is the one the
snapshot-on-subscribe path exists to keep small.

    first_quote_probe.py --addr localhost:9090 --samples 30
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
import uuid

PATH = "/tapeline.v1.MarketData/StreamQuotes"


def headers(key_id, secret):
    """Builds the four signed headers. Mirrors SignedRequest.canonicalString:
    method, path, timestamp, nonce and the hex SHA-256 of the body, joined by
    newlines. gRPC signs an empty body - request messages arrive after the
    headers, so signing the payload would mean buffering it before
    authenticating."""
    ts = int(time.time())
    nonce = str(uuid.uuid4())
    body_hash = hashlib.sha256(b"").hexdigest()
    canonical = "\n".join(["POST", PATH, str(ts), nonce, body_hash])
    sig = base64.b64encode(
        hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "x-tapeline-key": key_id,
        "x-tapeline-signature": sig,
        "x-tapeline-timestamp": str(ts),
        "x-tapeline-nonce": nonce,
    }


def sample(addr, key_id, secret, symbols, timeout):
    """Opens one stream and returns milliseconds to the first message, or None."""
    hdrs = []
    for k, v in headers(key_id, secret).items():
        hdrs += ["-H", f"{k}: {v}"]

    request = json.dumps({"symbols": symbols, "maxUpdatesHz": 10})
    cmd = ["grpcurl", "-plaintext", "-d", request, *hdrs, addr, PATH.lstrip("/")]

    started = time.monotonic()
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    try:
        # The first non-empty stdout line is the first message. grpcurl prints
        # a JSON object per received message, so this fires on arrival rather
        # than at stream close - which is precisely what the k6 path could not
        # do.
        for line in proc.stdout:
            if line.strip():
                return (time.monotonic() - started) * 1000.0
            if time.monotonic() - started > timeout:
                break
        return None
    finally:
        proc.kill()
        proc.wait(timeout=5)


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    idx = min(int(round((p / 100.0) * (len(s) - 1))), len(s) - 1)
    return s[idx]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", default="localhost:9090")
    ap.add_argument("--samples", type=int, default=30)
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--symbols", default="BTC-USD,ETH-USD")
    args = ap.parse_args()

    key_id = os.environ.get("TAPELINE_KEY_ID", "tk_local_dev")
    secret = os.environ.get(
        "TAPELINE_SECRET", "local-development-secret-not-for-production"
    )
    symbols = args.symbols.split(",")

    latencies, failures = [], 0
    for _ in range(args.samples):
        ms = sample(args.addr, key_id, secret, symbols, args.timeout)
        if ms is None:
            failures += 1
        else:
            latencies.append(ms)

    print(f"""
Time to first quote (cold connect)
==================================
samples                   {args.samples}
received a quote          {len(latencies)}
no quote within {args.timeout:.0f}s      {failures}
""")

    if latencies:
        print(f"p50                       {percentile(latencies, 50):.1f} ms")
        print(f"p95                       {percentile(latencies, 95):.1f} ms")
        print(f"max                       {max(latencies):.1f} ms")
        with open("first-quote-results.txt", "w") as f:
            f.write(f"samples={args.samples}\nreceived={len(latencies)}\n")
            f.write(f"p50_ms={percentile(latencies, 50):.1f}\n")
            f.write(f"p95_ms={percentile(latencies, 95):.1f}\n")
            f.write(f"max_ms={max(latencies):.1f}\n")

    print("""
Each sample is a separate process and connection, so this is cold
time-to-first-quote: dial, authenticate, subscribe, snapshot. It is not
comparable to a warm per-message latency and is not presented as one.
""")

    if not latencies:
        print("::error::no sample received a quote")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
