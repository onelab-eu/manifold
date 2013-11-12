from manifold.policy.target             import Target, TargetValue
from manifold.policy.target.cache.cache import Cache
from manifold.core.query                import Query
from manifold.core.record               import Record
from manifold.util.log                  import Log

class CacheTarget(Target):
    def __init__(self):
        # XXX This should be called when the router starts for initializing the
        # cache structure before the first query arrives
        self._cache = Cache()

    def process_query(self, query, annotations):
        #Log.tmp("CACHE - Processing query: %r, %r" % (query, annotations))
        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40
        records = self._cache.get_best_records(query, allow_processing=True)
        if not records is None:
            return (TargetValue.RECORDS, records)

        # Continue with the normal processing
        return (TargetValue.CONTINUE, None)

    def process_record(self, query, record, annotations):
        #print "*** CACHE: appending records into cache for query", query
        self._cache.append_records(query, record, create=True)
        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40
        return (TargetValue.CONTINUE, None)

    def process(self, query, record, annotations):
        if not record:
            return self.process_query(query, annotations)
        else:
            return self.process_record(query, record, annotations)
