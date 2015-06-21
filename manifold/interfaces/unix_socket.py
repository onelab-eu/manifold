#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

import os

from twisted.internet.protocol import Protocol, ServerFactory, ClientFactory
from twisted.protocols.basic import IntNStringReceiver

from manifold.interfaces            import Interface
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.util.filesystem       import ensure_writable_directory
from manifold.util.log              import Log
from manifold.util.reactor_thread   import ReactorThread
from manifold.interfaces.tcp_socket import TCPClientInterface, TCPServerSocketFactory

DEFAULT_FILENAME = '/var/run/manifold/manifold.sock'
DEFAULT_TIMEOUT = 10

class UNIXClientInterface(TCPClientInterface):

    __interface_type__ = 'unixclient'

    ############################################################################
    # Constructor

    def __init__(self, router, platform_name = None, **platform_config):
        if not platform_config:
            platform_config = dict()

        ensure_writable_directory(os.path.dirname(DEFAULT_FILENAME))
        self._connector = None
        TCPClientSocketFactory.__init__(self, router, platform_name, **platform_config)

    def parse_platform_config(self, platform_config):
        self._filename = platform_config.get('filename', DEFAULT_FILENAME)
        self._timeout = platform_config.get('timeout', DEFAULT_TIMEOUT)

    ############################################################################
    # State implementation 

    def down_impl(self):
        # Disconnect from the server
        if self._connector:
            self._connector.disconnect()

    def up_impl(self):
        self._connector = ReactorThread().connectUNIX(self._filename, self, timeout=self._timeout)

# This interface will spawn other interfaces
class UNIXServerInterface(Interface):

    __interface_type__ = 'unixserver'

    ############################################################################
    # Constructor / Destructor

    def __init__(self, router, platform_name = None, **platform_config):

        self.parse_platform_config(platform_config)

        ensure_writable_directory(os.path.dirname(self._filename))
        if os.path.exists(self._filename):
            Log.info("Removed existing socket file: %s" % (self._filename,))
            os.unlink(self._filename)

        ReactorThread().start_reactor()
        self._router    = router
        self._connector = None

        Interface.__init__(self, router, platform_name, **platform_config)

    def terminate(self):
        Interface.terminate(self)
        ReactorThread().stop_reactor()
        if os.path.exists(self._filename):
            os.unlink(self._filename)

    ############################################################################
    # Helpers

    def parse_platform_config(self, platform_config):
        self._filename = platform_config.get('filename', DEFAULT_FILENAME)

    ############################################################################
    # State implementation

    def up_impl(self):
        self._connector = ReactorThread().listenUNIX(self._filename, TCPServerSocketFactory(self._router))
        # XXX How to wait for the server to be effectively listening
        self.on_up()

    def down_impl(self):
        # Stop listening to connections
        ret = self._connector.stopListening()
        yield defer.maybeDeferred(ret)
        self.on_down()

    # We should be able to end all connected clients
