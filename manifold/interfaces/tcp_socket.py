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
        Log.info("RECV packet %r on interface %r" % (packet, self))
        self.receive(packet)

    def send_packet(self, packet):
        Log.info("SENT packet %r to interface %r" % (packet, self))
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

    def __init__(self, router, platform_name = None, platform_config = None, request_announces = True):
        Interface.__init__(self, router, platform_name, platform_config, request_announces)

        self._tx_buffer = list()
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
        self.set_up()

    def up_impl(self):
        # Cannot do much here...
        pass

    def down_impl(self):
        self.transport.loseConnection()
        # should trigger on_client_disconnected

    def on_client_disconnected(self, reason):
        self.set_down()
        self.get_router().unregister_interface(self)


################################################################################
# FACTORIES
################################################################################

# For the client, the factory is the interface. We have a single client
# interface and it is maintained through the various connections and
# disconnections
class TCPClientSocketFactory(TCPInterface, ClientFactory):
    """
    """
    protocol = ManifoldClientProtocol

    def on_client_connected(self, client):
        # This is used to disconnect, can't we just refer to transport for this ?
        self._client = client

        self.set_up(self._request_announces)

        # Send buffered packets
        while self._tx_buffer:
            full_packet = self._tx_buffer.pop()
            self.send(full_packet)

    def clientConnectionFailed(self, connector, reason):
        # reason = ConnectionRefusedError | ...
        self.set_error(reason)

    def on_client_disconnected(self, client, reason):
        self._client = None
        self.set_down()

    def send_impl(self, packet):
        if self.is_up():
            self._client.send_packet(packet)
        else:
            self._tx_buffer.append(packet)
        #assert self.is_up(), "We should not send packets to a disconnected interface"

class TCPServerSocketFactory(ServerFactory):
    """
    """
    protocol = ManifoldServerProtocol

    def __init__(self, router, platform_name, platform_config):
        self._router = router

    def buildProtocol(self, addr):
        p = self.protocol(self._router)
        p.factory = self
        return p


################################################################################
# Interfaces
################################################################################


# This is in fact the TCPClientSocketInterface..
# This is a bit weird but we need this to return an interface, and twisted
# protocols do not allow this.
class TCPClientInterface(TCPClientSocketFactory):

    __interface_type__ = 'tcpclient'

    def __init__(self, router, platform_name = None, platform_config = None, request_announces = True):
        if not platform_config:
            platform_config = dict()
        host = platform_config.get('host')
        port = platform_config.get('port', DEFAULT_PORT)
        timeout = platform_config.get('timeout', DEFAULT_TIMEOUT)

        self._host = host
        self._port = port
        self._timeout = timeout
        TCPClientSocketFactory.__init__(self, router, platform_name, platform_config, request_announces)

    def __repr__(self):
        return "<%s %s %s:%d>" % (self.__class__.__name__, self.get_platform_name(), self._host, self._port)

    def reconnect(self, host = None, port = DEFAULT_PORT):
        if self._client:
            self.down()
        if host:
            self._host = host
            self._port = port
        self.up()

    def down_impl(self):
        # Disconnect from the server
        if self._client:
            self._client.transport.loseConnection()

    def up_impl(self):
        ReactorThread().connectTCP(self._host, self._port, self)#, timeout=self._timeout)

    def get_description(self):
        if not self._client:
            return '(No client)'
        else:
            addr = self._client.transport.addr
            return 'Connected to %s:%s' % addr


# This interface will spawn other interfaces
class TCPServerInterface(Interface):
    """
    Server interface ( = serial port)
    This is only used to create new interfaces on the flight
    """
    __interface_type__ = 'tcpserver'

    def __init__(self, router, platform_name = None, platform_config = None, request_announces = True):

        if not platform_config:
            platform_config = dict()
        port = platform_config.get('port', DEFAULT_PORT)

        ReactorThread().start_reactor()
        self._port      = port

        Interface.__init__(self, router, platform_name, platform_config, request_announces)

    def terminate(self):
        Interface.terminate(self)
        ReactorThread().stop_reactor()

    def up_impl(self):
        ReactorThread().listenTCP(self._port, TCPServerSocketFactory(self._router, self._platform_name, self._platform_config))
        # XXX How to wait for the server to be effectively listening
        self.set_up(request_announces = False)

    @defer.inlineCallbacks
    def down_impl(self):
        # Stop listening to connections
        ret = self.transport.loseConnection()
        yield defer.maybeDeferred(ret)
        self.set_down()

    # We should be able to end all connected clients

    def get_description(self):
        if self.is_up():
            return 'Listening on %s' % (self._port,)
        else:
            return 'Not listening'

