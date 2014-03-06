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
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.operators.operator    import Operator
from manifold.util.log              import Log 
from manifold.util.type             import returns

DUMPSTR_RENAME = "RENAME %r" 

#------------------------------------------------------------------
# RENAME node
#------------------------------------------------------------------

class Rename(Operator):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, child, aliases):
        """
        Constructor.
        Args:
            child: The child Node.
            aliases: A dict {String : String} which translate field name used
                in the incoming Records into the corresponding field name
                in the output Records.
        """
        assert isinstance(aliases, dict),\
            "Invalid aliases = %s (%s)" % (aliases, type(aliases))
        # XXX Why ? -- jordan
        print "Disabled collision check in aliases"
        #assert set(aliases.keys()) & set(aliases.values()) == set(),\
        #    "Invalid aliases = %r (keys and values should be disjoint) (collisions on {%s})" % (
        #        aliases,
        #        set(aliases.keys()) & set(aliases.values())
        #    )

        Operator.__init__(self)
        ChildSlotMixin.__init__(self)
        self._aliases = aliases

        self._set_child(child)

    @returns(dict)
    def get_aliases(self):
        """
        Returns:
            A dictionnary {String : String} which maps the field name
            to rename with the corresponding updated field name.
        """
        return self._aliases

    @returns(StringTypes)
    def get_alias(self, field_name):
        """
        Args:
            field_name: A String related to a field name for which
                we want to get the alias.
        Returns:
            The corresponding alias if any, field_name otherwise.
        """
        try:
            return self.get_aliases(field_name)
        except KeyError:
            return field_name

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Operator.
        """
        return DUMPSTR_RENAME % self.get_aliases()

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive_impl(self, packet):
        """
        Handle an incoming Packet instance.
        Args:
            packet: A Packet instance.
        """
        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            # XXX need to remove the filter in the query
            new_packet = packet.clone()
            packet.update_query(Query.unfilter_by, self._filter)
            self._get_child().send(new_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            record = packet
            #record = { self._aliases.get(k, k): v for k, v in record.items() }
            try:
                for k, v in self._aliases.items():
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
                self.forward_upstream(record)
            except Exception, e:
                self.error("Error in Rename::receive: %s" % e)

        else: # TYPE_ERROR
            self.forward_upstream(packet)

    def dump(self, indent = 0):
        """
        Dump the this Rename instance to the standard output. 
        Args:
            indent: An integer corresponding to the number of spaces
                to write (current indentation).
        """
        super(Demux, self).dump(indent)
        self.get_producer().dump(indent + 1)
    
    @returns(Node)
    def optimize_selection(self, filter):
        """
        Propagate Selection Operator through this Operator.
        Args:
            filter: A Filter instance storing the WHERE clause.
        Returns:
            The root Operator of the optimized sub-AST.
        """
        Log.tmp("Not yet tested")
        # We must rename fields involved in filter
        # self.get_producer().optimize_selection(query, updated_filter)
        return self

    @returns(Node)
    def optimize_projection(self, fields):
        """
        Propagate Projection Operator through this Operator.
        Args:
            fields: A list of String correspoding the SELECTed fields.
        Returns:
            The root Operator of the optimized sub-AST.
        """
        Log.tmp("Not yet tested")
        # We must rename fields involved in fields
        self.get_producer().optimize_projection(query, updated_fields)
        return self
