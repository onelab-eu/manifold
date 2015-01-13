from manifold.clients.router            import ManifoldRouterClient
from manifold.core.annotation           import Annotation
from manifold.core.deferred_receiver    import DeferredReceiver
from manifold.core.packet               import Packet

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

        Log.warning("Hardcoded a GET packet")
        packet = GET()
        packet.set_destination(query.get_destination())
        packet.set_receiver(receiver) # Why is it useful ??
        packet.update_annotation(self.get_annotation())

        self.send(packet)

        return receiver.get_deferred()
