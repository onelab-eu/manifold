#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import sys, json
from json import JSONEncoder

from twisted.internet.protocol      import Protocol, Factory, ClientFactory
from twisted.internet               import defer
from twisted.protocols.basic        import IntNStringReceiver

from manifold.interfaces            import Interface
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread

# pip install autobahn
from autobahn.twisted.websocket import WebSocketServerProtocol, \
                                       WebSocketServerFactory

from manifold.core.packet           import GET
from manifold.core.query            import Query

DEFAULT_PORT = 9000

# http://stackoverflow.com/questions/3768895/python-how-to-make-a-class-json-serializable
class ManifoldJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Packet):
            return o.get_dict()
        else:
            return o.__dict__    

# TODO
# - clarify methods that an interface should implement

class ManifoldWebSocketServerProtocol(WebSocketServerProtocol, Interface):

    __interface_type__ = 'websocket'

    def __init__(self, router, platform_name = None, **platform_config):
        Interface.__init__(self, router, platform_name, **platform_config)
        self._client = None

        # Received packets are sent back to the client
        _self = self
        class MyReceiver(ChildSlotMixin):
            def receive(self, packet, slot_id = None):
                # To avoid this flow to be remembered
                packet.set_receiver(None)
                _self.send(packet)
        self._receiver = MyReceiver()

    def terminate(self):
        Interface.terminate(self)

    def send_impl(self, packet):
        # We assume we only send records...
        msg = json.dumps(packet.get_dict(), cls=ManifoldJSONEncoder)
        self.sendMessage(msg.encode(), False)

    def on_client_connected(self):
        print "On client connected... is onConnect called ?"
        self.on_up()

    def up_impl(self):
        # Cannot do much  here...
        pass

    def down_impl(self):
        self.transport.loseConnection()
        # should trigger on_client_disconnected

    def on_client_disconnected(self, reason):
        print "on_client_disconnected"
        self.on_down()
        self.get_router().unregister_interface(self)

    # -----

    def onConnect(self, request):
        print "onConnect... is on_client_connected called ?"
        print("Client connecting: {0}".format(request.peer))
        self.on_up()

    def onOpen(self):
        print("WebSocket connection open.")
        #self.sendMessage(u"bienvenue".encode(), False) # packet)

    def onMessage(self, payload, isBinary):
        if isBinary:
            print("Binary message received: {0} bytes".format(len(payload)))
        else:
            print("Text message received: {0}".format(payload.decode('utf8')))
            # We expect a JSON dict
            query_dict = json.loads(payload.decode('utf-8'))
            packet = GET()
            query = Query.from_dict(query_dict)
            packet.set_destination(query.get_destination())

            packet.set_receiver(self._receiver)
            Interface.receive(self, packet)

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))

class ManifoldWebSocketServerFactory(WebSocketServerFactory):
    protocol = ManifoldWebSocketServerProtocol

    def __init__(self, router, platform_name = None, **platform_config):
        port = platform_config.get('port', DEFAULT_PORT)

        WebSocketServerFactory.__init__(self, "ws://localhost:%s" % port, debug = False)
        self._router = router

    def buildProtocol(self, addr):
        p = self.protocol(self._router)
        p.factory = self
        return p

class ManifoldWebSocketServerInterface(Interface):
    """
    """
    __interface_type__ = 'websocketserver'

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
        factory = ManifoldWebSocketServerFactory(self._router, self._port)
        ReactorThread().listenTCP(self._port, factory)
        # XXX How to wait for the server to be effectively listening
        self.on_up()

    @defer.inlineCallbacks
    def down_impl(self):
        # Stop listening to connections
        ret = self.transport.loseConnection()
        yield defer.maybeDeferred(ret)
        self.on_down()
        # XXX We should be able to end all connected clients
