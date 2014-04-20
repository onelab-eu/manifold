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

from manifold.core.destination      import Destination
from manifold.core.node             import Node
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.record           import Records
from manifold.operators.operator    import Operator
from manifold.operators.subquery    import SubQuery
from manifold.util.log              import Log 
from manifold.util.type             import returns

DUMPSTR_RENAME = "RENAME %r" 

#------------------------------------------------------------------
# RENAME node
#------------------------------------------------------------------

class Rename(Operator, ChildSlotMixin):

    #---------------------------------------------------------------------------
    # Constructors
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
        Log.debug("Disabled collision check in aliases")
        #assert set(aliases.keys()) & set(aliases.values()) == set(),\
        #    "Invalid aliases = %r (keys and values should be disjoint) (collisions on {%s})" % (
        #        aliases,
        #        set(aliases.keys()) & set(aliases.values())
        #    )

        Operator.__init__(self)
        ChildSlotMixin.__init__(self)
        self._aliases = aliases

        self._set_child(child)

    def copy(self):
        return Rename(self._get_child().copy(), self._aliases.copy())

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(dict)
    def get_aliases(self):
        """
        Returns:
            A dictionnary {String : String} which maps the field name
            to rename with the corresponding updated field name.
        """
        return self._aliases


    # XXX ?
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

    def update_aliases(self, function):
        for k, v in self._aliases.items():
            new_k, new_v = function(k, v)
            if new_k:
                self._aliases.pop(k)
                self._aliases[new_k] = new_v
        

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

    @returns(Destination)
    def get_destination(self):
        """
        Returns:
            The Destination corresponding to this Operator. 
        """
        d = self._get_child().get_destination()
        rmap = {v: k for k, v in self.get_aliases().items()}
        return d.rename(self.get_aliases())

    def process_query(self, packet):
        # Do we really need to clone the packet ? We might modify the query in
        # place...
        new_packet = packet.clone()

        # Build a reverse map of renaming aliases. The map is optimized for
        # translating records, which are supposed to be more numerous.
        rmap = {v: k for k, v in self.get_aliases().items()}

        # NOTE: Steps 1) and 2) deal with the destination

        destination = packet.get_destination()

        # 1) Process filter
        destination.get_filter().rename(rmap)

        # 2) Process fields
        destination.get_fields().rename(rmap)

        packet.set_destination(destination)

        # 3) Process params (move to a Params() class ?)
        # XXX This is about the "applicative layer"
        params = packet.get_query().get_params()
        for key in params.keys():
            if key in rmap:
                params[rmap[key]] = params.pop(key)

        # XXX Process records: uuids now...
        #records = packet.get_records()
        #if records:
        #    packet.set_records(self.process_records(records))

        return packet

    def process_record(self, record):
        """
        This function modifies the record packet in place.

        NOTES:
         . It might be better to iterate on the record fields
         . It seems we only handle two levels of hierarchy. This is okay I think
        since in the query plan, further layers will be broken down across
        several subqueries.
        """

        if record.is_empty():
            return record

        # UGLY... can we shorten/simplify this a bit ???
        def rec(in_, out_, r):
            """
            Modifies r in place !
            """
            #print "REC(%r, %r, %r)" % (in_, out_, r)
            if isinstance(r, list):
                return map(lambda rr: rec(in_, out_, rr), r)

            if len(in_) == 0:
                if len(out_) == 0:
                    # / -> /
                    return r

                elif len(out_) == 1:
                    # / -> x
                    x = out_[0]
                    return r[x]

                else: # len(out_) > 1:
                    x = out_[0]
                    r[x] = rec([], out_[1:], old)
                    return r

            elif len(in_) == 1:
                if len(out_) == 0:
                    # a -> /
                    a = in_[0]
                    return r.pop(a, None)

                elif len(out_) == 1:
                    # a -> x
                    a, x = in_[0], out_[0]
                    r[x] = r.pop(a, None)
                    return r

                else: # len(out_) > 1:
                    # a -> x.y...
                    a, x = in_[0], out_[0]
                    old = r.pop(a, None)
                    r[x] = rec([], out_[1:], old)
                    return r

            else: # len(in_) > 1:
                if len(out_) == 0:
                    # a.b... -> /
                    a = in_[0]
                    old = r.pop(a, None)
                    if insinstance(old, list):
                        ret = list()
                        for x in old:
                            ret.extend(rec(in_[1:], [], x))
                        return ret
                    else: # dict
                        return rec(in_[1:], [], old)

                elif len(out_) == 1:
                    # a.b... -> x
                    a, x = in_[0], out_[0]
                    old = r.pop(a, None)
                    r[x] = rec(in_[1:], [], old)
                    return r

                else: # len(out_) > 1:
                    # a.b... -> x.y...
                    a, x = in_[0], out_[0]

                    old = r.pop(a, None)
                    r[x] = rec(in_[1:], out_[1:], old)
                    return r


        for k, v in self.get_aliases().items():
            in_ = k.split('.')
            out_ = v.split('.')

            qq = rec(in_, out_, record)

#DEPRECATED|            # we can have dots everywhere: eg. hops.probes.ip -> hops.ip
#DEPRECATED|            if k in record:
#DEPRECATED|                if '.' in v: # users.hrn
#DEPRECATED|                    method, key = v.split('.')
#DEPRECATED|                    # Careful to cases such as hops --renamed--> hops.ttl
#DEPRECATED|                    if not method in record:
#DEPRECATED|                        record[method] = Records() 
#DEPRECATED|                    for x in record[k]:
#DEPRECATED|                        record[method].append({key: x})        
#DEPRECATED|                else:
#DEPRECATED|                    record[v] = record.pop(k)
        return record

    def process_records(self, records):
        """
        This function replaces the list of records in the (query) packet.
        """
        return Records([self.process_record(r) for r in records])

    def receive_impl(self, packet):
        """
        Handle an incoming Packet instance.
        Args:
            packet: A Packet instance.
        """
        if packet.get_protocol() == Packet.PROTOCOL_QUERY:
            new_packet = self.process_query(packet)
            self._get_child().receive(new_packet)

        elif packet.get_protocol() == Packet.PROTOCOL_RECORD:
            new_packet = self.process_record(packet)
            self.forward_upstream(new_packet)

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
        rmap = {v: k for k, v in self.get_aliases().items()}
        new_filter = filter.copy().rename(rmap)
        self._update_child(lambda c, d: c.optimize_selection(new_filter))
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
        rmap = {v: k for k, v in self.get_aliases().items()}
        new_fields = fields.copy().rename(rmap)
        self._update_child(lambda c, d: c.optimize_projection(new_fields))
        return self

    @returns(Node)
    def reorganize_create(self):
        return self._get_child().reorganize_create()

    #---------------------------------------------------------------------------
    # Algebraic rules
    #---------------------------------------------------------------------------

    def subquery(self, ast, relation):
        """
        SQ_new o Rename
        """

        if relation.is_local():
            # Pass the subquery into the child, no renaming involved
            self._update_child(lambda l, d: l.subquery(ast, relation))
            return self

        # Default behaviour : SQ on top
        return SubQuery.make(self, ast, relation)
