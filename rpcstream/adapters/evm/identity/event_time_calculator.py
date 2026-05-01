from datetime import datetime
import time


class EventTimeCalculator:
    def calculate_event_timestamp(self, item):
        """
        Extract blockchain timestamp
        """
        if item is None or not isinstance(item, dict):
            return None

        # block
        if item.get("type") == "block":
            ts = item.get("timestamp")

        # others (tx/log/trace/etc)
        else:
            ts = item.get("block_timestamp")

        if ts is None:
            return None

        return self._to_rfc3339(ts)

    def calculate_event_timestamp_ms(self, item):
        """
        Extract blockchain event time in milliseconds.
        """
        if item is None or not isinstance(item, dict):
            return None

        if item.get("type") == "block":
            ts = item.get("timestamp")
        else:
            ts = item.get("block_timestamp")

        if ts is None:
            return None

        return int(ts) * 1000


    def calculate_ingest_timestamp(self):
        """
        System ingestion time
        """
        # Use epoch seconds directly to avoid naive-datetime timezone skew.
        return int(time.time() * 1000)


    def _to_rfc3339(self, ts):
        return datetime.utcfromtimestamp(int(ts)).isoformat() + "Z"
