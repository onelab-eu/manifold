import sys, time, uuid as uuid_module

from manifold.core.annotation       import Annotation
from manifold.core.destination      import Destination
from manifold.core.filter           import Filter
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import GET # to deprecate
from manifold.core.packet           import Record, Records
from manifold.util.async            import async_sleep
from manifold.util.log              import Log
from manifold.util.plugin_factory   import PluginFactory
from manifold.util.predicate        import Predicate
from manifold.util.misc             import lookahead

from twisted.internet               import defer
from manifold.util.reactor_thread   import ReactorThread

DEFAULT_TIMEOUT = 4
TIMEOUT_PER_TTL = 0.25
RECONNECTION_DELAY = 10

#from manifold.interfaces.tcp_socket     import TCPSocketInterface
#from manifold.interfaces.unix_socket    import UNIXSocketInterface

# XXX We need to keep track of clients to propagate announces

class FlowEntry(object):
    def __init__(self, receiver, last, timestamp, timeout):
        self._receiver  = receiver
        self._last      = last
        self._timestamp = timestamp
        self._timeout   = timeout
        self._expired   = False

    def get_receiver(self):
        return self._receiver

    def is_last(self):
        return self._last

    def get_timestamp(self):
        return self._timestamp

    def set_timestamp(self, timestamp):
        self._timestamp = timestamp

    def get_timeout(self):
        return self._timeout

    def set_expired(self, expired = True):
        self._expired = True

    def is_expired(self):
        return self._expired

class FlowMap(object):
    def __init__(self, interface):
        self._interface = interface

        self._map  = dict()  # hash by flow
        self._list = list()  # sort by expiry time

        self._timer_id = None
        self._on_tick()

    def terminate(self):
        self._stop_timer()

    def add_receiver(self, packet, receiver):
        now = time.time()

        timeout = DEFAULT_TIMEOUT - packet.get_ttl() * TIMEOUT_PER_TTL
        if timeout < TIMEOUT_PER_TTL:
            timeout = TIMEOUT_PER_TTL

        flow = packet.get_flow().get_reverse()
        last = packet.is_last()

        if flow in self._map:
            flow_entry = self._map[flow]
            if flow_entry.is_last():
                print "ignored duplicated flow", flow
                return False
            else:
                flow_entry.set_expired(False)
                flow_entry.set_timestamp(now)
                #print "unset expired for existing flow"
        else:
            self._map[flow] = FlowEntry(receiver, last, now, timeout)
            #print "add flow", now

        # With a default timeout, we always push back new entries, and no need
        # to reschedule
        if not self._list:
            self._set_timer(timeout)

        # Insert in the list by expiration time
        expiry = now + timeout
        if not flow in self._list:
            for pos, cur_flow in enumerate(self._list):
                cur_entry = self._map[cur_flow]
                if cur_entry.get_timestamp() + cur_entry.get_timeout() > expiry:
                    self._list.insert(pos, flow)
                    return True
            self._list.append(flow)

        return True

    def get(self, packet):
        flow = packet.get_flow()

        flow_entry = self._map.get(flow)
        if not flow_entry:
            return None

        if packet.is_last():
            del self._map[flow]
            self._list.remove(flow)
            # If the flow was not the first, maybe no need to reschedule
            self._reschedule()

        return flow_entry

    def _expire_flow(self, flow):
        record = Record(last = True)
        record.set_source(flow.get_source())
        record.set_destination(flow.get_destination())
        record._ingress = self._interface.get_address()

        receiver = self._map[flow].get_receiver()
        self._map[flow].set_expired()

        # XXX Code duplicated
        if receiver:
            receiver.receive(record)
        else:
            self._interface.get_router().receive(record)

    def _expire_flows(self):
        now = time.time()
        for flow in self._list:
            flow_entry = self._map[flow]
            delay = flow_entry.get_timeout() - now + flow_entry.get_timestamp()
            if delay < 0:
                del self._list[0]
                self._expire_flow(flow)
            else:
                return delay
        return 0 # no more flows

    def _reschedule(self):
        self._stop_timer()
        self._on_tick()

    def _on_tick(self, *args, **kwargs):
        #print "tick!", args, kwargs
        delay = self._expire_flows()
        if delay > 0:
            self._set_timer(delay)

    def _set_timer(self, delay):
        self._timer_id = ReactorThread().callLater(delay, self._on_tick, None)

    def _stop_timer(self):
        if self._timer_id:
            self._timer_id.cancel()

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
            self.send_impl(packet)

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

    def send_impl(self, packet):
        Log.error("Platform %s/%s : packet discarded %r" %
                (self.get_interface_type(), self._platform_name, packet))
        pass

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

        flow_entry = self._flow_map.get(packet)
        if flow_entry:
            if flow_entry.is_expired():
                Log.info("Received packet for expired flow. Discarding.")
                return
            receiver = flow_entry.get_receiver()
        else:
            do_send = self._flow_map.add_receiver(packet, receiver)
            if not do_send: # UGLY: to avoid sending duplicates, past last, packets
                print "do send is false prevent out"
                return

        #print "[OUT]", self, packet
        
        packet.inc_ttl()

        if self.is_up():
            self.send_impl(packet)
        else:
            self._tx_buffer.append(packet)

    def receive(self, packet, slot_id = None):
        """
        For packets received from the remote server."
        """
        #print "[ IN]", self, packet

        packet._ingress = self.get_address()

        flow_entry = self._flow_map.get(packet)
        if flow_entry:
            if flow_entry.is_expired():
                Log.info("Received packet for expired flow. Discarding.")
                return
            receiver = flow_entry.get_receiver()
        else:

            do_send = self._flow_map.add_receiver(packet, None)
            if not do_send: # UGLY: to avoid sending duplicates, past last, packets
                print "NOT SEND"
                return
            receiver = None

        if not receiver:
            # New flows are sent to the router
            print "packet to router", self._router, packet
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

