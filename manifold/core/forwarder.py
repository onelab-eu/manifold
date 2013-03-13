from manifold.core.interface    import Interface
from manifold.core.query_plan  import QueryPlan

class Forwarder(Interface):

    def forward(self, query, deferred=False, user=None):
        qp = QueryPlan()
        qp.build_simple(query, self.metadata)
        return qp.execute()


