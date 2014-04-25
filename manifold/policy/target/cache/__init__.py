from manifold.policy.target             import Target, TargetValue
from manifold.policy.target.cache.cache import Cache
from manifold.core.query                import Query
from manifold.core.record               import Record
from manifold.util.log                  import Log

class CacheTarget(Target):

    def process_query(self, query, annotations):
        #Log.tmp("CACHE - Processing query: %r, %r" % (query, annotations))

        # TODO: ROUTERV2
        # Cache per user
        cache = self._interface.get_cache(annotations)

        # If Query action is not get (Create, Update, Delete)
        # Invalidate the cache and propagate the Query        

        # TODO: cache.invalidate XXX to be implemented
        Log.tmp("-----------------> ACTION = %s",query.get_action())
        if query.get_action() != 'get':
            Log.tmp("--------------> Trying to Invalidate Cache")
            #cache.invalidate_entry(query)
            self._interface.delete_cache(annotations)
            # TODO: ROUTERV2
            # Cache per user
            cache = self._interface.get_cache(annotations)
            return (TargetValue.CONTINUE, None)

        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40
        if query.object.startswith('local:'):
            return (TargetValue.CONTINUE, None)
        
        records = cache.get_best_records(query, allow_processing=True)
        if not records is None:
            return (TargetValue.RECORDS, records)

        # Continue with the normal processing
        return (TargetValue.CONTINUE, None)

    def process_record(self, query, record, annotations):
        #print "*** CACHE: appending records into cache for query", query

        # TODO: ROUTERV2
        # Cache per user
        cache = self._interface.get_cache(annotations)
        cache.append_records(query, record, create=True)
        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40
        return (TargetValue.CONTINUE, None)

    def process(self, query, record, annotations):
        if not record:
            return self.process_query(query, annotations)
        else:
            return self.process_record(query, record, annotations)
