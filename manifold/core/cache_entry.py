import time
from manifold.core.record import LastRecord

class Entry(object):
    """
    Cache entry.

    An entry needs to represent 5 states, that will impact cache hits and related processing
    
    0. NO ENTRY : Query has not been seen 'recently'

    A new query comes
    1 .ENTRY NO RECORDS, EMPTY PENDING [] = QUERY IN PROGRESS

    Records start arriving
    2. ENTRY NO RECORDS, PENDING = QUERY RECEIVED RECORDS

    All records arrive
    3. ENTRY RECORDS, PENDING=None = QUERY DONE, NONE IN PROGRESS

    Query is being updated and no records yet(we set pending to [])
    4. ENTRY RECORDS, EMPTY PENDING = QUERY DONE, IN PROGRESS !!!! how to distinguish from previously

    5. ENTRY with RECORDS AND PENDING RECORDS = QUERY DONE, ANOTHER IN PROGRESS, ALREADY RECEIVED RECORDS

    All records arrive
    back to 3.
    """

    # XXX What happends in case of error
    # XXX How to distinguish from query with no record
    # XXX What about incomplete queries

    def __init__(self, records=None):
        self._created  = time.time()
        self._updated  = self._created
        self._accessed = None
        self._records  = records if records else list()
        self._pending_records = list() # Empty list means a query has been started
        self._operators = list() # A list of operators interested in our records

    # This is equivalent to the child_callback
    def set_records(self, records):
        if not isinstance(records, list):
            records = [records]
        self._pending_records = None
        self._records = records
        self._updated = time.time()
        for operator in self._operators:
            for record in records:
                operator.child_callback(record)
            operator.child_callback(LastRecord())

    def has_query_in_progress(self):
        return self._pending_records is not None

    def has_pending_records(self):
        return self._pending_records

    def append_record(self, record):
        if record.is_last():
            # Move all pending records to records...
            self._records = self._pending_records
            self._pending_records = None # None means no query started
            # ... and inform interested operators
        else:
            # Add the records in the pending list...
            self._pending_records.append(record)
            # ... and inform interested operators

        for operator in self._operators:
            operator.child_callback(record)

        self._updated = time.time()

    def get_records(self):
        self._accessed = time.time()
        return self._records

    def add_operator(self, operator):
        self._operators.append(operator)
    
    def remove_operator(self, operator):
        self._operators.remove(operator)
