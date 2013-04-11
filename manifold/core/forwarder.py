from manifold.core.result_value import ResultValue
from manifold.core.interface    import Interface
from manifold.core.query_plan   import QueryPlan
from manifold.core.result_value import ResultValue
class Forwarder(Interface):

    # XXX This could be made more generic with the router

    def forward(self, query, deferred=False, user=None):

        namespace = None
        # Handling internal queries
        if ':' in query.fact_table:
            namespace, table = query.fact_table.rsplit(':', 2)
        if namespace == self.LOCAL_NAMESPACE:
            q = copy.deepcopy(query)
            q.fact_table = table
            print "LOCAL QUERY TO STORAGE"
            # XXX should be about the current platform only
            return Storage.execute(q, user=user)
        elif namespace == "metadata":
            # XXX Should be about the current platform only
            raise Exception, "metadata not implemented"
        elif namespace:
            raise Exception, "Unsupported namespace '%s'" % namespace
        
        qp = QueryPlan()
        qp.build_simple(query, self.metadata, self.allowed_capabilities)
        self.instanciate_gateways(qp, user)
        results = qp.execute()
        return ResultValue.get_result_value(results, qp.get_result_value_array())
