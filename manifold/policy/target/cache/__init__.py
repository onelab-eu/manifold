from manifold.policy.target             import Target, TargetValue
from manifold.policy.target.cache.cache import Cache
from manifold.core.query                import Query
from manifold.core.record               import Record
from manifold.util.log                  import Log

class CacheTarget(Target):

    def process_query(self, query, annotations):
        #Log.tmp("CACHE - Processing query: %r, %r" % (query, annotations))

        if query.object.startswith('local:'):
            return (TargetValue.CONTINUE, None)

        # Cache per user
        cache = self._interface.get_cache(annotations)

        # If Query action is not get (Create, Update, Delete)
        # Invalidate the cache and propagate the Query        

        # TODO: cache.invalidate XXX to be implemented
        #Log.tmp("-----------------> ACTION = %s",query.get_action())
        if query.get_action() != 'get':
            #Log.tmp("--------------> Trying to Invalidate Cache")
            #cache.invalidate_entry(query)
            self._interface.delete_cache(annotations)
            # Cache per user
            cache = self._interface.get_cache(annotations)
            return (TargetValue.CONTINUE, None)

        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40

        # This will return a query plan in fact... not records
        allow_processing = annotations.get('cache') != 'exact'
        query_plan = cache.get_best_query_plan(query, allow_processing)
        if query_plan:
            return (TargetValue.CACHE_HIT, query_plan)

        # Continue with the normal processing
        return (TargetValue.CONTINUE, None)

    def process_record(self, query, record, annotations):
        #print "*** CACHE: appending records into cache for query", query

        # TODO: ROUTERV2
        # Cache per user
        cache = self._interface.get_cache(annotations)
        # No need to create an entry, since the entry is created when query arrives
        cache.append_records(query, record) 
        #, create=True)

        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40
        return (TargetValue.CONTINUE, None)

    def process(self, query, record, annotations):
        if not record:
            return self.process_query(query, annotations)
        else:
            return self.process_record(query, record, annotations)
