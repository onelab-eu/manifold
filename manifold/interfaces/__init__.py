import sys, time, uuid as uuid_module

from manifold.core.annotation       import Annotation
from manifold.core.address          import Address
from manifold.core.filter           import Filter
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import TimeoutErrorPacket
from manifold.core.record           import Record, Records
from manifold.util.async            import async_sleep
from manifold.util.log              import Log
from manifold.util.plugin_factory   import PluginFactory
from manifold.util.predicate        import Predicate
from manifold.util.misc             import lookahead

from twisted.internet               import defer

from flow_entry                     import FlowEntry
from flow_map                       import FlowMap

RECONNECTION_DELAY = 10


class Interface(object):

    __metaclass__ = PluginFactory
    __plugin__name__attribute__ = '__interface_type__'

    STATE_DOWN = 0
    STATE_PENDING_UP = 1
    STATE_UP = 2
    STATE_PENDING_DOWN = 3

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
        Log.info("Registered interfaces are: {%s}" % ", ".join(sorted(Interface.factory_list().keys())))

    # XXX Replace router by packet_callback
    # XXX Register interface elsewhere
    def __init__(self, router, platform_name = None, **platform_config):
        self._router   = router
        self._platform_name     = platform_name if platform_name else str(uuid_module.uuid4())
        self._platform_config   = platform_config

        self._tx_buffer = list()

        self._state    = self.STATE_DOWN
        self._error    = None # Interface has encountered an error

        self._up_callbacks   = list()
        self._down_callbacks = list()

        self._reconnecting = True
        self._reconnection_delay = RECONNECTION_DELAY

        # We use a flow map since at the moment, many receivers can ask for
        # queries to the interface directly without going through the router.
        # It is thus necessary to send the resulting packets back to the
        # requesting instances, and not to the router.
        self._flow_map = FlowMap(self)

        router.register_interface(self)

    def terminate(self):
        self.set_down()

    def set_reconnecting(self, reconnecting):
        self._reconnecting = reconnecting

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.get_platform_name())

    def get_platform_name(self):
        return self._platform_name

    def get_address(self):
        return Address('uuid', Filter().filter_by(Predicate('uuid', '==', self._platform_name)))

    def get_interface_type(self):
        return self.__interface_type__

    def get_router(self):
        return self._router

    def get_description(self):
        return ''

    def get_status(self):
        return 'UP' if self.is_up() else 'ERROR' if self.is_error() else 'DOWN'

    # Request the interface to be up...
    def set_up(self):
        self.unset_error()
        self._state = self.STATE_PENDING_UP
        self.up_impl()

    def up_impl(self):
        # Nothing to do, overload this in children interfaces
        self.on_up()

    # The interface is now up...
    def on_up(self):
        Log.info("Platform %s/%s: new state UP." %
                (self.get_interface_type(), self._platform_name,))
        self._state = self.STATE_UP

        # Send buffered packets
        if self._tx_buffer:
            Log.info("Platform %s/%s: sending %d buffered packets." %
                    (self.get_interface_type(), self._platform_name, len(self._tx_buffer)))
        while self._tx_buffer:
            packet = self._tx_buffer.pop()
            self.send_impl(packet, None)

        # Trigger callbacks to inform interface is up
        for cb, args, kwargs in self._up_callbacks:
            cb(self, *args, **kwargs)

    def set_down(self):
        self._state = self.STATE_PENDING_DOWN
        self.down_impl()

    def down_impl(self):
        # Nothing to do, overload this in children interfaces
        self.on_down()

    @defer.inlineCallbacks
    def on_down(self):
        Log.info("Platform %s/%s: new state DOWN." % (self.get_interface_type(), self._platform_name,))
        self._state = self.STATE_DOWN
        # Trigger callbacks to inform interface is down
        for cb, args, kwargs in self._down_callbacks:
            cb(self, *args, **kwargs)
        if self.is_error() and self._reconnecting:
            Log.info("Platform %s/%s: attempting to set it up againt in %d s." %
                    (self.get_interface_type(), self._platform_name, self._reconnection_delay))
            yield async_sleep(self._reconnection_delay)
            Log.info("Platform %s/%s: attempting reinit." %
                    (self.get_interface_type(), self._platform_name,))
            self.set_up()

    def set_error(self, reason):
        Log.info("Platform %s/%s: error [%s]." % (self.get_interface_type(), self._platform_name, reason))
        self._error = reason
        self.on_down()

    def unset_error(self):
        self._error = None

    def is_up(self):
        return self._state in [self.STATE_UP, self.STATE_PENDING_DOWN]

    def is_down(self):
        return self._state in [self.STATE_DOWN, self.STATE_PENDING_UP]

    def is_error(self):
        return self.is_down() and self._error is not None

    def reinit_impl(self):
        pass

    def reinit(self, **platform_config):
        self.set_down()
        if platform_config:
            self.reconnect_impl(self, **platform_config)
        self.set_up()

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

#    def send_impl(self, packet):
#        Log.info("Platform %s/%s : packet discarded %r" %
#                (self.get_interface_type(), self._platform_name, packet))
#        pass
#
    def _manage_outgoing_flow(self, packet):
        receiver = packet.get_receiver()
        flow_entry = self._flow_map.get(packet)
        if flow_entry:
            Log.error("Existing flow entry going out. This is not expected. We might do the same request multiple times though...")
            return False

        # We don't create a flow entry for outgoing packets with no receiver
        if receiver:
            # See last comment in next function
            self._flow_map.add_receiver(packet, receiver)
        return True

    def _manage_incoming_flow(self, packet):
        flow_entry = self._flow_map.get(packet)
        if flow_entry:
            # Those packets match a previously flow entry.
            receiver = flow_entry.get_receiver()
            packet.set_receiver(receiver)

            if packet.is_last():
                # We don't delete right now in case we have packets for this flow past the LAST
                # TODO a purge mechanism should take care of this
                self._flow_map.delete(packet)

            return receiver
        else:
            # We need to create a flow entry so that it can be deleted when the answer comes back
            # If we don't, a flow entry will be created by the outgoing reply, with receiver = None, that will prevent future incoming queries
            # Better solution in fact, check receiver before creating outgoing flow entry
            Log.info("Incoming flow on interface %r: sending to router" % (self,))
            return self._router

    def send(self, packet, orig_packet = None, source = None, destination = None, receiver = None):
        """
        Receive handler for orig_packets arriving from the router.

        When parameters are specified, they override orig_packet parameters.

        Since we might be multiplexing several flows, we need to remember the
        receiver for potential responses.
        """
        #print "[SEND]", self, packet
        new_source = packet.get_source()
        if source:
            new_source = source
        if not new_source and orig_packet:
            new_source = orig_packet.get_destination()
        if not new_source:
            new_source = self.get_address()
        if not new_source:
            raise Exception, "A orig_packet should have a source address"
        packet.set_source(new_source)

        new_destination = packet.get_destination()
        if destination:
            new_destination = destination
        if not new_destination and orig_packet:
            new_destination = orig_packet.get_source()
        if not new_destination:
            raise Exception, "A orig_packet should have a destination address"
        packet.set_destination(new_destination)

        new_receiver = packet.get_receiver()
        if receiver:
            new_receiver = receiver
        if not new_receiver and orig_packet:
            new_receiver = orig_packet.get_receiver()
        if not new_receiver:
            Log.warning("Receiver set to self ? Is it the right choice ?")
            new_receiver = self
        # XXX is it needed ?
        packet.set_receiver(new_receiver)

        # Why is it needed ?
        packet._ingress = self.get_address()

        if not self._manage_outgoing_flow(packet):
            return

        packet.inc_ttl()

        if self.is_up():
            Log.warning("We have to get rid of receiver now that we have a flow table")
            new_receiver.receive(packet)
        else:
            self._tx_buffer.append(packet)


    def receive(self, packet, slot_id = None):
        """
        For packets received from outside (eg. a remote server).
        """
        #print "[RECEIVE]", self, packet

        # This packet has been received by the current interface
        packet._ingress = self.get_address()

        # Determine if this packet is an answer to another packet -> check the flow table
        if not receiver:
            return

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

        self.send(record, packet)

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
        #socket = self.get_socket(QueryFactory.from_packet(packet))

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

        has_last = False
        for record, last in lookahead(records):
            has_last = True
            self.record(record, packet, last=last)
        if not has_last:
            self.last(packet)

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


    def error(self, packet, description, is_fatal = True):
        """
        Craft an ErrorPacket carrying an error message.
        Args:
            description: The corresponding error message (String) or
                Exception.
            is_fatal: Set to True if this ErrorPacket
                must make crash the pending Query.
        """
        # Could be factorized with Gateway::error() by defining Producer::error()
        print "error packet making"
        error_packet = self.make_error(CORE, description, is_fatal)
        self.send(error_packet, packet)
