from manifold.core.result_value import ResultValue
from manifold.core.interface    import Interface
from manifold.core.query_plan   import QueryPlan
from manifold.core.result_value import ResultValue
class Forwarder(Interface):

    # XXX This could be made more generic with the router
    # Forwarder class is an Interface 
    # builds the query plan, instanciate the gateways and execute query plan using deferred if required
    def forward(self, query, is_deferred=False, user=None):
        super(Router, self).forward(query, is_deferred, execute, user)

        # We suppose we have no namespace from here
        qp = QueryPlan()
        qp.build_simple(query, self.metadata, self.allowed_capabilities)
        self.instanciate_gateways(qp, user)
        d = defer.Deferred() if is_deferred else None
        # the deferred object is sent to execute function of the query_plan
        return qp.execute(d)
