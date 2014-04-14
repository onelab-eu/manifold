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
        self._cache_user = dict()

    # TODO: ROUTERV2 
    # this function creates a cache per user if user_id is in annotations
    # else it provides a global cache for non logged in Queries
    def get_cache(self, annotations):
        try:
            #Log.tmp("----------> CACHE PER USER <------------")
            #Log.tmp(annotations)
            user_id = annotations['user']['user_id']
            if 'user_id' not in self._cache_user:
                self._cache_user[user_id] = Cache()
            return self._cache_user[user_id]
        except:
            #Log.tmp("----------> NO CACHE PER USER <------------")
            import traceback
            traceback.print_exc()
            return self._cache

    def process_query(self, query, annotations):
        #Log.tmp("CACHE - Processing query: %r, %r" % (query, annotations))
        cache = self.get_cache(annotations)
        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40
        if query.object.startswith('local:'):
            return (TargetValue.CONTINUE, None)
        
        # If Query action is not get (Create, Update, Delete)
        # Invalidate the cache and propagate the Query        

        # TODO: cache.invalidate XXX to be implemented
        if query.get_action() != 'get':
            #cache.invalidate_entry(query)
            return (TargetValue.CONTINUE, None)

        records = cache.get_best_records(query, allow_processing=True)
        if not records is None:
            return (TargetValue.RECORDS, records)

        # Continue with the normal processing
        return (TargetValue.CONTINUE, None)

    def process_record(self, query, record, annotations):
        #print "*** CACHE: appending records into cache for query", query
        cache = self.get_cache(annotations)
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
