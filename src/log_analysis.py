"""
Apache Log MapReduce engine (INTE 22253 - Part C, Option 2).

This file contains ONLY the processing logic. It does not know anything
about Tkinter, so it can be used from the UI, from tests, or from the
command line.

Pipeline:
    READ INPUT -> SPLIT -> MAP (in N worker processes) -> SHUFFLE -> REDUCE

NOTE: The "workers" are separate processes on ONE computer. This simulates
distributed workers; it is not a real multi-machine cluster.
"""

import os
import re
import time
from collections import defaultdict
from datetime import datetime
from multiprocessing import Process, Queue
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_FILE = BASE_DIR.parent / "data" / "Apache.log"

# Example line:
# [Sun Nov 27 00:50:47 2005] [error] [client 61.155.76.2] Directory index forbidden by rule: /var/www/html/
#  \______ timestamp _____/  \level/  \____ optional IP ___/ \_____________ message _______________________/
LINE_PATTERN = re.compile(
    r"^\[(?P<timestamp>[^\]]+)\] "        # [Sun Nov 27 00:50:47 2005]
    r"\[(?P<level>[a-z]+)\] "             # [error]
    r"(?:\[client (?P<ip>[0-9.]+)\] )?"   # [client 61.155.76.2]   (optional)
    r"(?P<message>.*)$"                   # rest of the line
)

# Known error types. If a message CONTAINS the phrase on the left,
# it is grouped under that phrase. Order matters: first match wins.
KNOWN_ERROR_TYPES = [
    "File does not exist",
    "Directory index forbidden by rule",
    "script not found or unable to stat",
    "mod_jk child workerEnv in error state",
    "mod_jk child init",
    "Can't find child",
    "Factory error creating",
    "Can't create",
    "request failed: URI too long",
    "request failed: error reading the headers",
    "attempt to invoke directory as script",
    "Attempt to serve directory",
    "Invalid URI in request",
    "Invalid method in request",
    "client sent HTTP/1.1 request without hostname",
    "uri must start with /",
    "mod_security: Access denied",
]

# Keys used for the intermediate (key, value) pairs.
LEVEL = "LEVEL"
ERROR_TYPE = "ERROR_TYPE"
CLIENT_IP = "CLIENT_IP"
ERROR_DATE = "ERROR_DATE"
RECORD = "RECORD"        # RECORD:parsed / RECORD:malformed  (so no line is silently lost)


# ---------------------------------------------------------------------------
# 1. Parsing helpers
# ---------------------------------------------------------------------------

def parse_log_line(line):
    """Parse one log line into a dict, or return None if it is malformed."""
    match = LINE_PATTERN.match(line.strip())
    if match is None:
        return None
    return {
        "timestamp": match.group("timestamp"),
        "level": match.group("level"),
        "client_ip": match.group("ip"),        # None if the line has no [client ...]
        "message": match.group("message"),
    }


def classify_error(message):
    """Turn a raw error message into a stable error type.

    'Directory index forbidden by rule: /var/www/html/' -> 'Directory index forbidden by rule'
    """
    for error_type in KNOWN_ERROR_TYPES:
        if error_type in message:
            return error_type
    # Unknown message: keep the text before the first ':' and hide numbers,
    # so values like process IDs do not create thousands of "different" errors.
    cleaned = message.split(":")[0]
    cleaned = re.sub(r"\d+", "N", cleaned)
    return cleaned.strip() or "Unknown error"


def timestamp_to_date(timestamp):
    """'Sun Nov 27 00:50:47 2005' -> '2005-11-27'"""
    return datetime.strptime(timestamp, "%a %b %d %H:%M:%S %Y").strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# 2. SPLIT
# ---------------------------------------------------------------------------

def read_records(input_file):
    """Read the log file and return a list of lines (one record per line)."""
    with open(input_file, "r", encoding="utf-8", errors="replace") as f:
        return [line.rstrip("\n") for line in f if line.strip()]


def split_records(records, num_workers):
    """Split records into num_workers chunks of almost equal size.

    Example: 10 records, 3 workers -> chunk sizes 4, 3, 3
    """
    if num_workers < 1:
        raise ValueError("num_workers must be at least 1")
    chunk_size, remainder = divmod(len(records), num_workers)
    chunks = []
    start = 0
    for i in range(num_workers):
        # The first `remainder` chunks get one extra record.
        end = start + chunk_size + (1 if i < remainder else 0)
        chunks.append(records[start:end])
        start = end
    return chunks


# ---------------------------------------------------------------------------
# 3. MAP
# ---------------------------------------------------------------------------

def map_logs(records):
    """MAP: turn each record into (key, 1) pairs.

    A key is a tuple (category, value), e.g.
        (("LEVEL", "error"), 1)
        (("ERROR_TYPE", "Directory index forbidden by rule"), 1)
        (("CLIENT_IP", "61.155.76.2"), 1)
    """
    pairs = []
    for line in records:
        parsed = parse_log_line(line)
        if parsed is None:
            pairs.append(((RECORD, "malformed"), 1))
            continue

        pairs.append(((RECORD, "parsed"), 1))
        pairs.append(((LEVEL, parsed["level"]), 1))

        if parsed["level"] == "error":
            pairs.append(((ERROR_TYPE, classify_error(parsed["message"])), 1))
            pairs.append(((ERROR_DATE, timestamp_to_date(parsed["timestamp"])), 1))
            if parsed["client_ip"]:
                pairs.append(((CLIENT_IP, parsed["client_ip"]), 1))
    return pairs
