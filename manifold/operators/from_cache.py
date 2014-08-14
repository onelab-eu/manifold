from manifold.operators     import Node
from manifold.core.record   import LastRecord

DUMPSTR_FROMCACHE  = "CACHE [%s]"

class FromCache(Node):
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

    def __init__(self, query, cache_entry):
    
        super(FromCache, self).__init__()
        self.query = query
        self._cache_entry = cache_entry

        # A cache entry is an operator node
        # XXX self.set_callback() ???? XXX We will have multiple callbacks in general
        # XXX self.query = ??

    def start(self): 

        # Will receive a start when executed == when the source is ready to receive records
        # That's when we make the difference between different modes (cached, buffered, multicast)
        if self._cache_entry.has_query_in_progress():
            print "query in progress"
            if self._cache_entry.has_pending_records():
                print "has pending records"
                # Let's first return all pending records, then wait for
                # set_records to receive further records
                print "pending records = ", self._cache_entry._pending_records
                for record in self._cache_entry._pending_records:
                    record.set_annotation('cache', 'buffered multicast')
                    self.send(record)

            else:
                # Query did not started, just return new incoming records
                # Nothing to do, let's wait for set_records
                pass

            # Inform the cache entry we are interested in its records
            self._cache_entry.add_operator(self)

        else:
            # If we reached here, we _have_ records in cache
            # otherwise, we would have a cache entry and no query in progress,
            # and no query ever done == INCONSISTENT
            for record in self._cache_entry.get_records():
                record.set_annotation('cache', 'cache')
                self.send(record)
            record = LastRecord()
            record.set_annotation('cache', 'cache')
            self.send(record)

    def __repr__(self, indent = 0):
        """
        Returns:
            The "%s" representation of this FromTable Node.
        """
        return DUMPSTR_FROMCACHE % (self.get_query(), )

    # This is equivalent to the child_callback
    def child_callback(self, record):
        record.set_annotation('cache', 'multicast')
        self.send(record)

    # This should not be needed
    def optimize_selection(self):
        raise NotImplemented
    def optimize_projection(self):
        raise NotImplemented
