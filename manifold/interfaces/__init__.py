import sys, uuid

from manifold.core.annotation       import Annotation
from manifold.core.destination      import Destination
from manifold.core.filter           import Filter
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import GET
from manifold.util.log              import Log
from manifold.util.plugin_factory   import PluginFactory
from manifold.util.predicate        import Predicate

#from manifold.interfaces.tcp_socket     import TCPSocketInterface
#from manifold.interfaces.unix_socket    import UNIXSocketInterface

# XXX We need to keep track of clients to propagate announces

class Interface(object):

    __metaclass__ = PluginFactory
    __plugin__name__attribute__ = '__interface_name__'

    # XXX This should be in PluginFactory
    @staticmethod
    def register_all(force = False):
        """
        Register each available Manifold Gateway if not yet done.
        Args:
            force: A boolean set to True enforcing Gateway registration
                even if already done.
        """
        # XXX We should not need such test... it's a coding error and should
        # raise a Fatal exception
        Log.info("Registering interface")
        current_module = sys.modules[__name__]
        PluginFactory.register(current_module)
        Log.info("Registered interface are: {%s}" % ", ".join(sorted(Interface.list().keys())))

    def __init__(self, router):
        self._router   = router
        self._uuid     = str(uuid.uuid4())
        self._flow_map = dict()

        router.register_interface(self)

    def get_uuid(self):
        return self._uuid

    def up(self):
        pass

    def down(self):
        pass

    def terminate(self):
        pass

    def _request_announces(self):
        self.send(GET(), destination=Destination('object', namespace='local'), receiver = self._router.get_fib())

    def send_impl(self, packet, destination, receiver):
        raise NotImplemented

    def send(self, packet, source = None, destination = None, receiver = None):
        """
        Receive handler for packets arriving from the router.
        For packets coming from the client, directly use the router which is
        itself a receiver.
        """
        # XXX This code should be shared by all interfaces

        if not source:
            if not packet.get_source():
                source = Destination('uuid', Filter().filter_by(Predicate('uuid', '==', self._uuid)))
                packet.set_source(source)
        else:
            packet.set_source(source)

        if destination:
            packet.set_destination(destination)

        if receiver:
            receiver_id = str(uuid.uuid4())
            packet.set_receiver(receiver_id)
            self._flow_map[packet.get_flow()] = receiver

        #print "[OUT]", packet
        self.send_impl(packet, destination, receiver)

    def receive(self, packet):
        """
        For packets received from the remote server."
        """
        #print "[ IN]", packet
        # XXX Not all packets are targeted at the router.
        # - announces are
        # - supernodes are not (they could eventually pass through the router)

        flow = packet.get_flow()

        if not flow in self._flow_map:
            # New flows are sent to the router
            self._router.receive(packet)
        else:
            # Existing flows rely on the state defined in the router... XXX
            self._flow_map[flow].receive(packet)
