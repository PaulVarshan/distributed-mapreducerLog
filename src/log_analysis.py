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
