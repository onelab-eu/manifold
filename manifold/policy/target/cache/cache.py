from manifold.util.singleton            import Singleton
from manifold.util.lattice              import Lattice
from manifold.policy.target.cache.entry import Entry
from manifold.core.query_plan           import QueryPlan

class Cache(object):

    __metaclass__ = Singleton

    # Cache = query -> entry

    def __init__(self):
        self._lattice = Lattice()

    def get_entry(self, query):
        return self._lattice.get_data(query)

    def add_entry(self, query, entry):
        self._lattice.add(query, entry)

    def append_records(self, query, records, create=False):
        if not isinstance(records, list):
            records = [records]
        entry = self.get_entry(query)
        if not entry:
            if not create:
                raise Exception, "Query not found in cache: %r" % query
            entry = Entry()
            self.add_entry(query, entry)
        entry.set_records(records)

    def get_best_records(self, query, allow_processing = False):
        best_query_entry_tuple = self._lattice.get_best(query)
        if not best_query_entry_tuple:
            return None
        best_query, best_entry = best_query_entry_tuple
        records = best_entry.get_records()
        if best_query == query:
            return records
        else:
            # We need to add processing
            # XXX This could be optimized and made into a function into QueryPlan
            #print "** Building records from bigger query"
            qp = QueryPlan()
            qp.ast.from_table(query, records, key = None).selection(query.get_where()).projection(query.get_select())
            return qp.execute()
    

    def dump(self):
        return self._lattice.dump()
