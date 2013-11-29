#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# An Demux Node forwards incoming Records to several parent Nodes. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.fr> 

from types                          import StringTypes

from manifold.core.query            import Query
from manifold.operators             import ChildStatus, ChildCallback
from manifold.operators.operator    import Operator 
from manifold.util.type             import returns

#------------------------------------------------------------------
# DEMUX node
#------------------------------------------------------------------

class Demux(Operator):

    def __init__(self, child):
        """
        Constructor
        Args:
            child A Node instance, child of this Demux Node.
        """
        super(Demux, self).__init__()
        self.child = child
        #TO FIX self.status = ChildStatus(self.all_done)
        self.child.set_callback(ChildCallback(self, 0))
        self.query = self.child.get_query().copy()

#DEPRECATED|    def add_callback(self, callback):
#DEPRECATED|        """
#DEPRECATED|        Add a parent callback to this Node.
#DEPRECATED|        """
#DEPRECATED|        self.parent_callbacks.append(callback)
#DEPRECATED|
#DEPRECATED|    def callback(self, record):
#DEPRECATED|        """
#DEPRECATED|        Processes records received by the child Node.
#DEPRECATED|        Args:
#DEPRECATED|            record: A incoming Record;
#DEPRECATED|        """
#DEPRECATED|        for callback in self.parent_callbacks:
#DEPRECATED|            callbacks(record)

    @returns(StringTypes)
    def __repr__(self):
        return "DEMUX (built above %r)" % self.get_child() 

    def dump(self, indent = 0):
        """
        Dump the this Demux instance to the standard output. 
        Args:
            indent: An integer corresponding to the number of spaces
                to write (current indentation).
        """
        super(Demux, self).dump(indent)
        self.get_child().dump(indent + 1)

#DEPRECATED|    def start(self):
#DEPRECATED|        """
#DEPRECATED|        Propagates a START message through the node
#DEPRECATED|        """
#DEPRECATED|        self.child.start()
#DEPRECATED|        self.status.started(0)

    def get_child(self):
        """
        Returns:
            A Node instance (the child Node) of this Demux instance.
        """
        return self.child

    def add_parent(self, parent):
        """
        Add a parent Node to this Demux Node.
        Args:
            parent: A Node instance.
        """
        assert issubclass(Node, type(parent)), "Invalid parent %r (%r)" % (parent, type(parent))
        print "not yet implemented"

    def optimize_selection(self, filter):
        self.child = self.child.optimize_selection(filter)
        return self

    def optimize_projection(self, fields):
        # We only need the intersection of both
        self.child = self.child.optimize_projection(fields)
        return self.child
