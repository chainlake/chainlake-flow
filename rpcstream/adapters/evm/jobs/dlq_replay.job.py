#!/usr/bin/env python3
from __future__ import annotations

import os
import sys


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "../../../.."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from rpcstream.adapters.evm.jobs.dlq_replay_job import cli


if __name__ == "__main__":
    cli()
