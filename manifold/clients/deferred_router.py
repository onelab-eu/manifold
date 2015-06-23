from manifold.clients.router            import ManifoldRouterClient
from manifold.core.annotation           import Annotation
from manifold.core.deferred_receiver    import DeferredReceiver
from manifold.core.packet               import Packet
from manifold.util.log                  import Log

class ManifoldDeferredRouterClient(ManifoldRouterClient):
    
    def forward(self, query, annotation = None):
        """
        Send a Query to the nested Manifold Router.
        Args:
            query: A Query instance.
            annotation: The corresponding Annotation instance (if
                needed) or None.
        Results:
            The ResultValue resulting from this Query.
        """
        if not annotation:
            annotation = Annotation()
        annotation |= self.get_annotation() 

        receiver = DeferredReceiver()
        #packet = QueryPacket(query, annotation, receiver = receiver)

        packet = Packet()
        packet.set_protocol(query.get_protocol())
        data = query.get_data()
        if data:
            packet.set_data(data)

        packet.set_destination(query.get_destination())
        packet.update_annotation(self.get_annotation())
        packet.set_receiver(receiver) # Why is it useful ??
        self._router.receive(packet)

        return receiver.get_deferred()
