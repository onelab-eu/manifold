import sys, uuid as uuid_module

from manifold.core.annotation       import Annotation
from manifold.core.destination      import Destination
from manifold.core.filter           import Filter
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import GET # to deprecate
from manifold.core.packet           import Record, Records
from manifold.util.log              import Log
from manifold.util.plugin_factory   import PluginFactory
from manifold.util.predicate        import Predicate
from manifold.util.misc             import lookahead

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

    def __init__(self, router, platform_name = None, platform_config = None, request_announces = False):
        self._router   = router
        self._platform_name     = platform_name if platform_name else str(uuid_module.uuid4())
        self._platform_config   = platform_config
        self._request_announces = request_announces
        self._up       = False
        self._error    = None # Interface has encountered an error
        self._up_callbacks = list()
        self._down_callbacks = list()

        # We use a flow map since at the moment, many receivers can ask for
        # queries to the interface directly without going through the router.
        # It is thus necessary to send the resulting packets back to the
        # requesting instances, and not to the router.
        self._flow_map = dict()

        router.register_interface(self)

    def terminate(self):
        self.down()

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.get_platform_name())

    def get_platform_name(self):
        return self._platform_name

    def get_address(self):
        return Destination('uuid', Filter().filter_by(Predicate('uuid', '==', self._platform_name)))

    def get_interface_type(self):
        return self.__interface_type__

    def get_router(self):
        return self._router

    def get_description(self):
        return ''

    def get_status(self):
        return 'UP' if self.is_up() else 'ERROR' if self.is_error() else 'DOWN'

    # Request the interface to be up...
    def up(self):
        self.up_impl()

    # Overload this in children interfaces
    def up_impl(self):
        self.set_up()

    # The interface is now up...
    def set_up(self, request_announces = True):
        if request_announces:
            self.request_announces()
        self._up = True
        for cb, args, kwargs in self._up_callbacks:
            cb(self, *args, **kwargs)
        
    def down(self):
        self._up = False
        self.down_impl()

    def down_impl(self):
        self.set_down()

    def set_down(self):
        self._up = False
        for cb, args, kwargs in self._down_callbacks:
            cb(self, *args, **kwargs)

    def set_error(self, reason):
        self._error = reason

    def unset_error(self):
        self._error = None

    def is_up(self):
        return self._up

    def is_down(self):
        return not self.is_up()

    def is_error(self):
        return self._error is not None

    def add_up_callback(self, callback, *args, **kwargs):
        cb_tuple = (callback, args, kwargs)
        self._up_callbacks.append(cb_tuple) 

    def del_up_callback(self, callback):
        self._up_callbacks = [cb for cb in self._up_callbacks if cb[0] == callback]
        
    def add_down_callback(self, callback, *args, **kwargs):
        cb_tuple = (callback, args, kwargs)
        self._down_callbacks.append(cb_tuple) 

    def del_down_callback(self, callback):
        self._down_callbacks = [cb for cb in self._down_callbacks if cb[0] == callback]

    def request_announces(self):
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
        #print "send flow", packet.get_flow()
        #try:
        #    print "send receiver", receiver
        #except: pass

        if receiver:
            self._flow_map[packet.get_flow()] = receiver

        #print "[OUT]", self, packet
        #print "*** FLOW MAP: %s" % self._flow_map
        #print "-----"
        
        self.send_impl(packet)

    def receive(self, packet, slot_id = None):
        """
        For packets received from the remote server."
        """
        #print "[ IN]", self, packet

        # For interfaces not exchanging announces, prevent it
        # XXX should add a self._answer_announces parameter instead
        if not self._request_announces:
            destination = packet.get_destination()
            if destination and destination.get_namespace() == 'local' and destination.get_object() == 'object':
                self.last(packet)
                return

        #print "*** FLOW MAP: %s" % self._flow_map
        #print "-----"
        packet._ingress = self.get_address()
        # XXX Not all packets are targeted at the router.
        # - announces are
        # - supernodes are not (they could eventually pass through the router)

        flow = packet.get_flow()
        receiver = self._flow_map.get(flow)

        #print "send flow", flow
        #try:
        #    print "send receiver", receiver
        #except: pass

        if not receiver:
            # New flows are sent to the router
            self._router.receive(packet)
        else:
            # Existing flows rely on the state defined in the router... XXX
            packet.set_receiver(receiver)
            receiver.receive(packet)

    #---------------------------------------------------------------------------
    # Helper functions
    #---------------------------------------------------------------------------

    def record(self, record, packet, last = None):
        """
        Helper used in Gateway when a has to send an ERROR Packet.
        See also Gateway::records() instead.
        Args:
            packet: The QUERY Packet instance which has triggered
                the call to this method.
            record: A Record or a dict instance. If this is the only
                Packet that must be returned, turn on the LAST_RECORD
                flag otherwise the Gateway will freeze.
                Example:
                    my_record = Record({"field" : "value"})
                    my_record.set_last(True)
                    self.record(my_record)
        """
        if not isinstance(record, Record):
            record = Record.from_dict(record)
        if last is not None:
            record.set_last(last)

        record.set_source(packet.get_destination())
        record.set_destination(packet.get_source())
        record._ingress = self.get_address()

        packet.get_receiver().receive(record)

    # XXX It is important that the packet is the second argument for
    # deferred callbacks
    def records(self, records, packet):
        """
        Helper used in Gateway when a has to send several RECORDS Packet.
        Args:
            packet: The QUERY Packet instance which has triggered
                the call to this method.
            record: A Records or a list of instances that may be
                casted in Record (e.g. Record or dict instances).
        """
        #socket = self.get_socket(Query.from_packet(packet))

        # Print debugging information
        # TODO refer properly pending Socket of each Gateway because
        # that's why we do not simply run socket.get_producer().format_uptree()
        #Log.debug(
        #    "UP-TREE:\n--------\n%s\n%s" % (
        #        socket.get_producer().format_node(),
        #        socket.format_uptree()
        #    )
        #)

        if not records:
            self.last(packet)
            return

        for record, last in lookahead(records):
            self.record(record, packet, last=last)
            
    def last(self, packet):
        self.record(Record(last = True), packet)

    def warning(self, packet, description):
        """
        Helper used in Gateway when a has to send an ERROR Packet
        carrying an Warning. See also Gateway::error()
        Args:
            packet: The QUERY Packet instance which has triggered
                the call to this method.
            description: The corresponding warning message (String) or
                Exception.
        """
        self.error(packet, description, False)

