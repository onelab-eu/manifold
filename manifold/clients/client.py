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

from manifold.core.annotation       import Annotation
from manifold.core.packet           import Packet, QueryPacket
from manifold.core.result_value     import ResultValue
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.log              import Log 
from manifold.util.type             import accepts, returns

class ManifoldClient(object):

    def __init__(self):
        """
        Constructor
        """
        pass

    def __del__(self):
        """
        Shutdown gracefully self.router 
        """
        pass

    @returns(StringTypes)
    def welcome_message(self):
        """
        Method that should be overloaded and used to log
        information while running the Query (level: INFO).
        Returns:
            A welcome message
        """
        raise NotImplementedError

    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        raise NotImplementedError

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
        raise NotImplementedError

