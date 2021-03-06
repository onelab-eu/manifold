from manifold.util.singleton            import Singleton
from manifold.util.lattice              import Lattice
from manifold.core.cache_entry          import Entry
from manifold.core.query_plan           import QueryPlan
from manifold.util.log                  import Log

class Cache(object):

    # TODO: ROUTERV2
    # Cache per user
    #__metaclass__ = Singleton

    # Cache = query -> entry

    def __init__(self):
        self._lattice = Lattice()

    def get_entry(self, query):
        return self._lattice.get_data(query)
    
    def invalidate_entry(self, query):
        return self._lattice.invalidate(query, recursive=True)

    def new_entry(self, query):
        self.add_entry(query, Entry())
    
    def add_entry(self, query, entry):
        self._lattice.add(query, entry)

    def last_record(self, query):
        entry = self.get_entry(query)
        if not entry:
            if not create:
                raise Exception, "Query not found in cache: %r" % query
            self.add_entry(query, Entry())
        entry.set_last_record()

    def append_records(self, query, records, create=False):
        entry = self.get_entry(query)
        if not entry:
            if not create:
                raise Exception, "Query not found in cache: %r" % query
            self.add_entry(query, Entry())
        entry.set_records(records)

    def append_record(self, query, record, create=False):
        # TMP CACHE DEBUG
        #import pdb
        #pdb.set_trace()

        entry = self.get_entry(query)
        if not entry:
            if not create:
                return
                #raise Exception, "Query not found in cache: %r" % query
            self.add_entry(query, Entry())
        entry.append_record(record)

    def get_best_query_plan(self, query, allow_processing = False):
        """
        Returns:
            A tuple status, records according to the state of queries in progress.

            status: cached, buffered, multicast, none
        """
        # TMP CACHE DEBUG
        #import pdb
        #pdb.set_trace()

        best_query_entry_tuple = self._lattice.get_best(query)
        if not best_query_entry_tuple:
            return None

        # We found a best entry. In all cases, we plug a query plan on top of the cache entry
        best_query, best_entry = best_query_entry_tuple

        query_plan = QueryPlan()
        query_plan.ast.from_cache(query, best_entry)
        if best_query != query:
            if not allow_processing:
                return None
            # We need to add processing
            query_plan.ast.selection(query.get_where()).projection(query.get_select())
            # XXX Shall we create a new entry for this query ? and all
            # intermediate steps ? see operator graph

        return query_plan

    def dump(self):
        return self._lattice.dump()
