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

    def process_query(self, query, annotation):
        #Log.tmp("CACHE - Processing query: %r, %r" % (query, annotation))
        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40
        if query.object.startswith('local:'):
            return (TargetValue.CONTINUE, None)

        records = self._cache.get_best_records(query, allow_processing=True)
        if not records is None:
            return (TargetValue.RECORDS, records)

        # Continue with the normal processing
        return (TargetValue.CONTINUE, None)

    def process_record(self, query, record, annotation):
        #print "*** CACHE: appending records into cache for query", query
        self._cache.append_records(query, record, create=True)
        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40
        return (TargetValue.CONTINUE, None)

    def process(self, query, record, annotation):
        if not record:
            return self.process_query(query, annotation)
        else:
            return self.process_record(query, record, annotation)
