#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ManifoldClient is a the base virtual class that
# inherits any Manifold client. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from types                          import StringTypes

from manifold.core.packet           import Packet
from manifold.core.result_value     import ResultValue
from manifold.core.sync_receiver    import SyncReceiver
from manifold.interfaces            import Interface
from manifold.util.log              import Log
from manifold.util.misc             import wait
from manifold.util.type             import accepts, returns


class ManifoldClient(object):

    def __init__(self, interface_type = None, **kwargs):
        """
        Constructor
        """
        self._interface_type = interface_type
        self._interface_args = kwargs
        if not interface_type:
            self._interface = None
            return

        interface_cls = Interface.factory_get(interface_type)
        if not interface_cls:
            Log.warning("Could not create a %(interface_type)s interface" % locals())
            return None

        self._receiver = self.make_receiver()

        self._interface = interface_cls(self._receiver, 'shell_interface', kwargs, request_announces = False)
        self._interface.up()
        wait(lambda : self._interface.is_up() or self._interface.is_error())

    def terminate(self):
        """
        Shutdown gracefully self.router 
        """
        if self._interface:
            self._interface.terminate()

    def make_receiver(self):
        receiver = SyncReceiver()
        receiver.register_interface = lambda x : None
        receiver.up_interface       = lambda x : None
        receiver.down_interface     = lambda x : None
        receiver.get_fib            = lambda   : None
        return receiver

    @returns(StringTypes)
    def welcome_message(self):
        """
        Method that should be overloaded and used to log
        information while running the Query (level: INFO).
        Returns:
            A welcome message
        """
        if self._interface_type:
            return "Shell using interface '%s' with parameters %r" % (self._interface_type, self._interface_args)
        else:
            return "Shell with no interface"

    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        raise Exception, "Authentication not supported yet!"

    @returns(ResultValue)
    def forward(self, query, annotation = None):
        """
        Send a Query to the nested Manifold Router.
        Args:
            query: A Query instance.
            annotation: The corresponding Annotation instance (if
                needed) or None.
        Results:
            The ResultValue resulting from this Query.
        """
        r = self.make_receiver()

        packet = Packet()
        packet.set_protocol(query.get_protocol())
        packet.set_protocol(query.get_protocol())
        data = query.get_data()
        if data:
            packet.set_data(data)

        packet.set_source(self._interface.get_address())
        packet.set_destination(query.get_destination())
        if annotation:
            packet.update_annotation(annotation)
        packet.set_receiver(r)

        self._interface._flow_map[packet.get_flow().get_reverse()] = r

        self._interface.send(packet)

        # This code is blocking
        result_value = r.get_result_value()
        assert isinstance(result_value, ResultValue),\
            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
        return result_value
