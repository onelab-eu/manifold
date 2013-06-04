from manifold.core.result_value import ResultValue
from manifold.core.interface    import Interface
from manifold.core.query_plan   import QueryPlan
from manifold.core.result_value import ResultValue
class Forwarder(Interface):

    # XXX This could be made more generic with the router

    def forward(self, query, deferred=False, user=None):
        super(Router, self).forward(query, deferred, execute, user)

        # We suppose we have no namespace from here
        qp = QueryPlan()
        qp.build_simple(query, self.metadata, self.allowed_capabilities)
        self.instanciate_gateways(qp, user)
        d = defer.Deferred() if deferred else None
        # the deferred object is sent to execute function of the query_plan
        return qp.execute(d)
