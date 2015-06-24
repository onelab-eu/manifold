#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold Interface class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.f>

from manifold.interfaces            import Interface

announce_str = """
class timeout {
    int timeout;
    CAPABILITIES(retrieve);
    KEY(timeout);
}
"""

class TimeoutInterface(Interface):
    """
    Server interface ( = serial port)
    This is only used to create new interfaces on the flight
    """
    __interface_type__ = 'test_timeout'

    ############################################################################
    # Constructor / Destructor

    def __init__(self, router, platform_name = None, **platform_config):
        Interface.__init__(self, router, platform_name, **platform_config)

    def send_impl(self, packet):
        destination = packet.get_destination()
        namespace = destination.get_namespace()
        object_name = destination.get_object_name()

        if namespace == 'test' and object_name == 'timeout':
            announces = Announces.from_string(announce_str)
            self.records(announces)
        pass
        
    def receive_impl(self, packet):
        pass
