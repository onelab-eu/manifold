#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# An Operator Node represents a SQL-like operation and
# is used to build an OperatorGraph. Operator forwards
# QUERY Packets received from its parents (Consumers) to its
# children (Producers). Resulting RECORD or ERROR Packets
# are sent back to its parents.
#
# Note:
# - An Operator producer may be either an Operator
# instance or a Socket.
# - An Operator consumer may be either an Operator
# instance or, if this is a From instance, a Gateway. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from manifold.core.packet import Packet
from manifold.core.query  import Query 
from manifold.core.relay  import Relay
from manifold.util.log    import Log
from manifold.util.type   import accepts, returns

# NOTES: it seem we don't need the query anymore in the operators expect From
# maybe ? Selection, projection ??

class Operator(Relay):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, producers = None, consumers = None, max_producers = None, max_consumers = None, has_parent_producer = False):
        """
        Constructor.
        Args:
            See relay::__init__()
        """
        Log.tmp(">>>>>>>>>>>>>>>>>>>> Operator")
        Relay.__init__( \
            self, \
            producers = producers, \
            consumers = consumers, \
            max_consumers = max_consumers, \
            max_producers = max_producers, \
            has_parent_producer = has_parent_producer \
        )

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        raise Exception, "Operator::receive() must be overwritten in children classes"
        
    @returns(Query)
    def get_query(self):
        raise Exception, "Operator::get_query() must be overwritten in children classes"

    def dump(self, indent = 0):
        """
        Dump the current node
        indent current indentation
        """
        self.tab(indent)
        print "%r (%s)" % (self, super(Operator, self).__repr__())
