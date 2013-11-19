#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Rename Node allows to rename field name(s) of
# input Records.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                          import StringTypes

from manifold.core.node             import Node
from manifold.operators.operator   import Operator
from manifold.util.log              import Log 
from manifold.util.type             import returns

DUMPSTR_RENAME = "RENAME %r" 

#------------------------------------------------------------------
# RENAME node
#------------------------------------------------------------------

class Rename(Operator):
    """
    RENAME operator node (cf SELECT clause in SQL)
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, child, map_fields):
        """
        Constructor
        """

        Operator.__init__(self, producers = child, max_producers = 1)
        self._map_fields = map_fields

    @returns(dict)
    def get_map_fields(self):
        """
        Returns:
            A dictionnary {String : String} which maps the field name
            to rename with the corresponding updated field name.
        """
        return self._map_fields


    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Node.
        """
        return DUMPSTR_RENAME % self.get_map_fields()


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        """
        """

        if packet.get_type() == Packet.TYPE_QUERY:
            # XXX need to remove the filter in the query
            new_packet = packet.clone()
            packet.update_query(Query.unfilter_by, self._filter)
            self.send(new_packet)

        elif packet.get_type() == Packet.TYPE_RECORD:
            record = packet

            if not record.is_last():
                #record = { self.map_fields.get(k, k): v for k, v in record.items() }
                try:
                    for k, v in self.map_fields.items():
                        if k in record:
                            if '.' in v: # users.hrn
                                method, key = v.split('.')
                                if not method in record:
                                    record[method] = list() 
                                for x in record[k]:
                                    record[method].append({key: x})        
                            else:
                                record[v] = record.pop(k) #record[k]
                            #del record[k]
                except Exception, e:
                    Log.error("Error in Rename::child_callback:", e)
                    import traceback
                    traceback.print_exc()
            self.send(record)

        else: # TYPE_ERROR
            self.send(packet)

    def dump(self, indent = 0):
        """
        Dump the current node
        Args:
            indent: An integer corresponding to the current indentation
                in number of spaces.
        """
        Node.dump(self, indent)
        self.get_producer().dump(indent + 1)
    
    @returns(Node)
    def optimize_selection(self, filter):
        """
        Propage WHERE operator through this RENAME Node.
        Args:
            filter: A Filter instance storing the WHERE clause.
        Returns:
            The update root Node of the optimized AST.
        """
        # TODO We must rename fields involved in filter
        Log.warning('Not implemented')
        return self

    @returns(Node)
    def optimize_projection(self, fields):
        """
        Propage SELECT operator through this RENAME Node.
        Args:
            fields: A list of String correspoding the SELECTed fields.
        Returns:
            The update root Node of the optimized AST.
        """
        # TODO We must rename fields involved in filter
        Log.warning('Not implemented')
        return self
