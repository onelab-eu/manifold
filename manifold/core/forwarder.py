from manifold.core.interface    import Interface
from manifold.core.query_plane  import QueryPlane

class Forwarder(Interface):

    def forward(self, query, deferred=False, user=None):
        qp = QueryPlane()
        qp.build_simple(query, self.metadata)
        return qp.execute()


