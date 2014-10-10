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
    __plugin__name__attribute__ = '__interface_type__'

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
        Log.info("Registered interface are: {%s}" % ", ".join(sorted(Interface.factory_list().keys())))

    def __init__(self, router):
        self._router   = router
        self._uuid     = str(uuid.uuid4())
        self._up       = False

        # We use a flow map since at the moment, many receivers can ask for
        # queries to the interface directly without going through the router.
        # It is thus necessary to send the resulting packets back to the
        # requesting instances, and not to the router.
        self._flow_map = dict()

        router.register_interface(self)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.get_interface_name())

    def get_uuid(self):
        return self._uuid
    get_platform_name = get_uuid
    get_interface_name = get_uuid

    def get_address(self):
        return Destination('uuid', Filter().filter_by(Predicate('uuid', '==', self._uuid)))

    def get_interface_type(self):
        return self.__interface_type__

    def up(self):
        self._up = True
        self._request_announces()
        self._router.up_interface(self)

    def down(self):
        self._up = False
        self._router.down_interface(self)

    def is_up(self):
        return self._up

    def is_down(self):
        return not self.is_up()


    def terminate(self):
        pass

    def _request_announces(self):
        fib = self._router.get_fib()
        if fib:
            self.send(GET(), source = fib.get_address(), destination=Destination('object', namespace='local'), receiver = fib)

    def send_impl(self, packet):
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
                source = self.get_address()
                packet.set_source(source)
        else:
            packet.set_source(source)

        if destination:
            packet.set_destination(destination)

        if not receiver:
            receiver = packet.get_receiver()
        else:
            packet.set_receiver(receiver)

        if receiver:
            self._flow_map[packet.get_flow()] = receiver

        print "[OUT]", self, packet
        #print "*** FLOW MAP: %s" % self._flow_map
        #print "-----"
        
        self.send_impl(packet)

    def receive(self, packet):
        """
        For packets received from the remote server."
        """
        print "[ IN]", self, packet
        #print "*** FLOW MAP: %s" % self._flow_map
        #print "-----"
        packet._ingress = self.get_address()
        # XXX Not all packets are targeted at the router.
        # - announces are
        # - supernodes are not (they could eventually pass through the router)

        flow = packet.get_flow()
        receiver = self._flow_map.get(flow)


        if not receiver:
            #print "packet has no receiver", packet
            # New flows are sent to the router
            self._router.receive(packet)
        else:
            # Existing flows rely on the state defined in the router... XXX
            receiver.receive(packet)
