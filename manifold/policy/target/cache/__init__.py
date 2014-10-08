from manifold.policy.target             import Target, TargetValue
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
        if query.get_action() != 'get':
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

        cache.new_entry(query)

        # Continue with the normal processing
        return (TargetValue.CONTINUE, None)

    def process_record(self, query, record, annotations):
        # We don't want to store records coming from cache in cache
        # -> let's be sure that records from cache are properly annotated by from_cache
        #
        # We don't want to store records mutiple times if process_records
        # is called from several matching cache targets.
        # -> returns ACCEPT and not CONTINUE

        # TODO: ROUTERV2
        # Cache per user
        cache = self._interface.get_cache(annotations)
        
        Log.warning("-------------------------")
        Log.tmp(query)
        Log.tmp(annotations)
        Log.warning("-------------------------")

        # No need to create an entry, since the entry is created when query arrives

        # We don't cache records whose origin is the cache
        if not 'cache' in record.get_annotations():
            cache.append_record(query, record)  # one or several records at once

        #print "==== DUMPING CACHE ====="
        #print self._cache.dump()
        #print "="*40
        # A cache target is a termination ! (no other choice)
        return (TargetValue.ACCEPT, None)

    def process(self, query, record, annotations, is_query):
        if is_query:
            return self.process_query(query, annotations)
        else:
            return self.process_record(query, record, annotations)
