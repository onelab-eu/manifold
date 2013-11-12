import time

class Entry(object):
    """
    Cache entry.
    """

    def __init__(self, records=None):
        self._created = time.time()
        self._updated = self._created
        self._records = records if records else []
        self._pending_records = []

    def set_records(self, records):
        if not isinstance(records, list):
            records = [records]
        self._records = records
        self._updated = time.time()

    def append_records(self, records):
        if not isinstance(records, list):
            records = [records]
        for record in records:
            if record.is_last():
                self._records = self._pending_records
                self._pending_records = []
                return
            self._pending_records.append(record)
        self._updated = time.time()

    def get_records(self):
        return self._records
