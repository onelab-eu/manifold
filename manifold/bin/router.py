#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Local router process reading on UNIX socket
#
# This file is part of the MANIFOLD project
#
# Copyright (C), UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Loïc Baron        <loic.baron@lip6.fr>

import asynchat, asyncore, os, socket, traceback
from types                          import StringTypes

from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.core.router           import Router
from manifold.core.sync_receiver    import SyncReceiver
from manifold.gateways              import Gateway
from manifold.util.daemon           import Daemon
from manifold.util.log              import Log
from manifold.util.options          import Options 
from manifold.util.type             import accepts, returns 

class State(object):
    pass

class QueryHandler(asynchat.async_chat, ChildSlotMixin):

    STATE_LENGTH = State()
    STATE_PACKET = State()

    def __init__ (self, conn, addr, callback):
        asynchat.async_chat.__init__ (self, conn)
        ChildSlotMixin.__init__(self) # XXX
        self.addr = addr

        # Reading socket data is done in two steps: first get the length of the
        # packet, then read the packet of a given length
        self.pstate = self.STATE_LENGTH
        self.set_terminator(8)
        self._receive_buffer = []
        self.callback = callback

    def collect_incoming_data (self, data):
        self._receive_buffer.append (data)

    def found_terminator (self):
        self._receive_buffer, data = [], ''.join(self._receive_buffer)

        if self.pstate is self.STATE_LENGTH:
            packet_length = int(data, 16)
            self.set_terminator(packet_length)
            self.pstate = self.STATE_PACKET
        else:
            self.set_terminator (8)
            self.pstate = self.STATE_LENGTH

            packet = Packet.deserialize(data)

            self.callback(self.addr, packet, receiver = self) or ""

    def receive(self, packet):
        packet_str = packet.serialize()
        self.push(('%08x' % len(packet_str)) + packet_str)

class RouterServer(asyncore.dispatcher):
    def __init__(self, socket_path):
        """
        Constructor.
        Args:
            socket_path: A String instance containing the absolute
                path of the socket used by this ManifoldServer. 
        """
        self._socket_path = socket_path
        asyncore.dispatcher.__init__(self)

        # Router initialization
        self._router = Router()

        # conflict when adding both
        self._router.add_platform('ping', 'ping_process')
        self._router.add_platform('paristraceroute', 'paristraceroute_process')

#DEPRECATED|        self._router.add_platform('agent',  'manifold', {'url': 'http://ple2.ipv6.lip6.fr:58000/RPC/'})
#DEPRECATED|        self._router.add_platform('agent2', 'manifold', {'url': 'http://planetlab2.cs.du.edu:58000/RPC/'})
        self._router.add_platform('maxmind', 'maxmind')

        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(self._socket_path)
        self.listen(128)

    @returns(StringTypes)
    def get_socket_path(self):
        """
        Returns:
            The absolute path of the socjet used by this ManifoldServer.
        """
        return self._socket_path

    def handle_accept(self):
        conn, addr = self.accept()
        QueryHandler(conn, addr, self.on_received)

    def on_received(self, addr, packet, receiver):
        packet.set_receiver(receiver)
        self._router.receive(packet)

    def terminate(self):
        """
        Stops gracefully this ManifoldServer.
        """
        self._router.terminate()
        if os.path.exists(self._socket_path):
            os.unlink(self._socket_path)

class RouterDaemon(Daemon):
    DEFAULTS = {
        "socket_path" : "/tmp/manifold" 
    }

    def __init__(self):
        """
        Constructor.
        """
        Daemon.__init__(
            self,
            self.terminate
        )

    def main(self):
        """
        Run ManifoldServer (called by Daemon::start).
        """
        Log.info("Starting RouterServer")

        # Preparing the RouterServer
        try:
            self._router_server = RouterServer(Options().socket_path)
        except Exception, e:
            Log.error(traceback.format_exc())
            raise e

        # Running the server
        try:
            asyncore.loop()
        finally:
            self._router_server.terminate()

    @staticmethod
    def init_options():
        """
        Prepare options supported by RouterDaemon.
        """
        options = Options()
        options.add_argument(
            "-S", "--socket", dest = "socket_path",
            help = "Socket that will read the Manifold router.",
            default = RouterDaemon.DEFAULTS["socket_path"]
        )

    def terminate(self):
        """
        Function called when the RouterDaemon must stops.
        """
        Log.info("Stopping RouterServer")
        try:
            self._router_server.terminate()
        except AttributeError:
            # self._router_server may not exists for instance if the
            # socket is already in use.
            pass

#DEPRECATED|Gateway.register_all()
#DEPRECATED|server = RouterServer()
#DEPRECATED|try:
#DEPRECATED|    asyncore.loop()
#DEPRECATED|finally:
#DEPRECATED|    if os.path.exists(RouterServer.path):
#DEPRECATED|        os.unlink(RouterServer.path)

def main():
    RouterDaemon.init_options()
    Log.init_options()
    Daemon.init_options()
    Options().parse()
    RouterDaemon().start()

if __name__ == "__main__":
    main()
