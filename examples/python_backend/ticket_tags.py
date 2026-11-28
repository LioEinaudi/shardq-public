"""Tag support requests using an ordered table of regular expressions."""

import json
import re
import time
from collections import Counter

from shardq.backends.base import Backend, Partition, RangeResult, session_fingerprint, validate_unit_ids
from shardq.fs import atomic_write_json


class TagSession:
    def __init__(self, tickets, rules, key):
        self.session_key = key
        self.tickets = tickets
        self.rules = [(rule["tag"], re.compile(rule["pattern"], re.IGNORECASE)) for rule in rules]
        self.ranges_completed = 0

    def run_range(self, partition, start, count, out_dir):
        began = time.monotonic()
        records = out_dir / "records.jsonl"
        with records.open("w", encoding="utf-8") as output:
            for unit_id in range(start, start + count):
                ticket = self.tickets[unit_id]
                tag = next((tag for tag, pattern in self.rules if pattern.search(ticket)), "other")
                output.write(json.dumps({"unit_id": unit_id, "text": ticket, "tag": tag}) + "\n")
        self.ranges_completed += 1
        return RangeResult(count, time.monotonic() - began, records,
                           extra={"session_range": self.ranges_completed})

    def close(self):
        pass


class TicketBackend(Backend):
    def describe(self):
        return [Partition("tickets", len(self.backend_args["tickets"]))]

    def session_key(self):
        # The session captures both the ordered rules and the indexed input.
        return session_fingerprint("ticket-tags-v1", self.backend_args["rules"], self.backend_args["tickets"])

    def open_session(self):
        return TagSession(self.backend_args["tickets"], self.backend_args["rules"], self.session_key())

    def validate_range(self, partition, start, count, result):
        validate_unit_ids(result.records_file, start, count)

    def merge(self, partition, records, out_dir, meta):
        counts = Counter(row["tag"] for row in records)
        atomic_write_json(out_dir / "results.json", {
            "by_tag": dict(counts), "completed_units": meta["completed_units"],
            "missing_units": meta["missing_units"],
        })
        return {}
