#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import struct

from twisted.internet.protocol import Protocol, ServerFactory, ClientFactory
from twisted.protocols.basic import IntNStringReceiver

from manifold.interfaces            import Interface
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread
from manifold.interfaces.tcp_socket import TCPClientSocketFactory, TCPServerSocketFactory

DEFAULT_FILENAME = '/var/run/manifold/manifold.sock'

class UNIXClientInterface(TCPClientSocketFactory):

    __interface_tyfpe__ = 'unixclient'

    def __init__(self, router, filename = DEFAULT_FILENAME):
        self._connector = None
        self._filename = filename
        self._timeout = timeout
        TCPClientSocketFactory.__init__(self, router)

    def reconnect(self, filename = DEFAULT_FILENAME):
        self.down()
        self._filename = filename
        self.up()

    def down_impl(self):
        # Disconnect from the server
        if self._connector:
            self._connector.disconnect()

    def up_impl(self):
        self._connector = ReactorThread().connectUNIX(self._filename, self)#, timeout=self._timeout)

# This interface will spawn other interfaces
class UNIXServerInterface(Interface):

    __interface_type__ = 'unixserver'

    def __init__(self, router, filename = DEFAULT_FILENAME):
        ReactorThread().start_reactor()
        self._router    = router
        self._filename  = filename
        self._connector = None
        Interface.__init__(self, router)

    def terminate(self):
        Interface.terminate(self)
        ReactorThread().stop_reactor()

    def _request_announces(self):
        pass

    def up_impl(self):
        self._connector = ReactorThread().listenUNIX(self._filename, TCPServerSocketFactory(self._router))
        # XXX How to wait for the server to be effectively listening
        self.set_up()

    def down_impl(self):
        # Stop listening to connections
        ret = self._connector.stopListening()
        yield defer.maybeDeferred(ret)
        self.set_down()

    # We should be able to end all connected clients
