#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import struct

from twisted.internet.protocol      import Protocol, ServerFactory, ClientFactory
from twisted.protocols.basic        import IntNStringReceiver
from twisted.internet               import defer
from twisted.internet.error         import ConnectionRefusedError

from manifold.interfaces            import Interface
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread

DEFAULT_PORT = 50000
DEFAULT_TIMEOUT = 10

################################################################################
# BASE CLASSES
################################################################################

# A protocol is recreated everytime a connection is made
# so it cannot be an interface...
class ManifoldProtocol(IntNStringReceiver):
    """ 
    Base class for building client and server protocols.

    The protocol is based on twisted.protocols.basic IntNStringReceiver, with
    little-endian 32-bit length prefix.
    """

    structFormat = "<L"
    prefixLength = struct.calcsize(structFormat)

    def connectionMade(self):
        self.on_client_connected()

    def stringReceived(self, msg):
        packet = Packet.deserialize(msg)
        Log.debug("RECV packet %r on interface %r" % (packet, self))
        self.receive(packet)

    def send_packet(self, packet):
        Log.debug("SENT packet %r to interface %r" % (packet, self))
        self.sendString(packet.serialize())

    def connectionLost(self, reason):
        #print "CONNECTION LOST: REASON:", reason, " - CLIENT", self
        self.on_client_disconnected(reason)
    # connection lost = client=None in factory

class TCPInterface(Interface):
    """
    Base class for building client and server interfaces.
    """

    __interface_type__ = 'socket'

    def __init__(self, router, platform_name = None, **platform_config):
        Interface.__init__(self, router, platform_name, **platform_config)

        # _client = None means interface is down
        self._client = None

        # Received packets are sent back to the client
        _self = self
        class MyReceiver(ChildSlotMixin):
            def receive(self, packet, slot_id = None):
                # To avoid this flow to be remembered
                packet.set_receiver(None)
                _self.send(packet)
        self._receiver = MyReceiver()

        ReactorThread().start_reactor()

    def terminate(self):
        Interface.terminate(self)
        ReactorThread().stop_reactor()

    def send_impl(self, packet):
        raise NotImplemented # Should be overloaded in children classes

    # We really are a gateway !!! A gateway is a specialized Interface that
    # answers instead of transmitting.
    # 
    # And a client, a Router are also Interface's, cf LocalClient

    # from protocol
    # = when we receive a packet from outside
    def receive(self, packet):
        packet.set_receiver(self._receiver)
        Interface.receive(self, packet)

    def get_description(self):
        if not self.transport:
            return '(No client)'
        else:
            return 'Connected from %s:%s' % self.transport.client



################################################################################
# PROTOCOLS
################################################################################

class ManifoldClientProtocol(ManifoldProtocol):

    # In the client, the interface is the factory. We forward the received
    # packet to the interface.
    def receive(self, packet):
        self.factory.receive(packet)

    def on_client_connected(self):
        self.factory.on_client_connected(self)

    def on_client_disconnected(self, reason):
        self.factory.on_client_disconnected(self, reason)


# For the server, the protocol is the interface
class ManifoldServerProtocol(ManifoldProtocol, TCPInterface):

    def send_impl(self, packet):
        self.send_packet(packet)

    def on_client_connected(self):
        self.on_up()

    def up_impl(self):
        # Cannot do much here...
        pass

    def down_impl(self):
        self.transport.loseConnection()
        # should trigger on_client_disconnected

    def on_client_disconnected(self, reason):
        self.on_down()
        self.get_router().unregister_interface(self)


################################################################################
# FACTORIES
################################################################################

class TCPServerSocketFactory(ServerFactory):
    """
    """
    protocol = ManifoldServerProtocol

    def __init__(self, router):
        self._router = router

    def buildProtocol(self, addr):
        p = self.protocol(self._router)
        p.factory = self
        return p


################################################################################
# Interfaces
################################################################################

# For the client, the factory is the interface. We have a single client
# interface and it is maintained through the various connections and
# disconnections
class TCPClientInterface(TCPInterface, ClientFactory):
    """
    """
    protocol = ManifoldClientProtocol
    __interface_type__ = 'tcpclient'

    ############################################################################
    # Constructor

    def __init__(self, router, platform_name = None, **platform_config):
        self.parse_platform_config(platform_config)

        TCPInterface.__init__(self, router, platform_name, **platform_config)

    ############################################################################
    # Helpers

    def parse_platform_config(self, platform_config):
        self._host = platform_config.get('host')
        self._port = platform_config.get('port', DEFAULT_PORT)
        self._timeout = platform_config.get('timeout', DEFAULT_TIMEOUT)

    def __repr__(self):
        return "<%s %s %s:%d>" % (self.__class__.__name__, self.get_platform_name(), self._host, self._port)

    def get_host(self):
        return self._host

    def reinit_impl(self, **platform_config):
        self.parse_platform_config(platform_config)

    def get_description(self):
        if not self._client:
            return '(No client)'
        else:
            addr = self._client.transport.addr
            return 'Connected to %s:%s' % addr

    ############################################################################
    # Events

    def on_client_connected(self, client):
        # This is used to disconnect, can't we just refer to transport for this ?
        self._client = client
        self.on_up()

    def clientConnectionFailed(self, connector, reason):
        # reason = ConnectionRefusedError | ...
        self.set_error(reason)

    def on_client_disconnected(self, client, reason):
        self._client = None
        self.on_down()

    ############################################################################
    # State implementation 

    def down_impl(self):
        # Disconnect from the server
        if self._client:
            self._client.transport.loseConnection()

    def up_impl(self):
        ReactorThread().connectTCP(self._host, self._port, self, timeout=self._timeout)

    ############################################################################
    # Packet processing

    def send_impl(self, packet):
        self._client.send_packet(packet)

# This interface will spawn other interfaces
class TCPServerInterface(Interface):
    """
    Server interface ( = serial port)
    This is only used to create new interfaces on the flight
    """
    __interface_type__ = 'tcpserver'

    ############################################################################
    # Constructor / Destructor

    def __init__(self, router, platform_name = None, **platform_config):

        self.parse_platform_config(platform_config)

        ReactorThread().start_reactor()

        Interface.__init__(self, router, platform_name, **platform_config)

    def terminate(self):
        Interface.terminate(self)
        ReactorThread().stop_reactor()

    ############################################################################
    # Helpers

    def parse_platform_config(self, platform_config):
        self._port = platform_config.get('port', DEFAULT_PORT)

    def get_description(self):
        if self.is_up():
            return 'Listening on %s' % (self._port,)
        else:
            return 'Not listening'

    ############################################################################
    # State implementation

    def up_impl(self):
        ReactorThread().listenTCP(self._port, TCPServerSocketFactory(self._router))
        # XXX How to wait for the server to be effectively listening
        self.on_up()

    @defer.inlineCallbacks
    def down_impl(self):
        # Stop listening to connections
        ret = self.transport.loseConnection()
        yield defer.maybeDeferred(ret)
        self.on_down()
        # XXX We should be able to end all connected clients
