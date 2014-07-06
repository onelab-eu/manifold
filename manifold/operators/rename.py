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
from manifold.core.fields           import FIELD_SEPARATOR
from manifold.core.node             import Node
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.record           import Record, Records
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
from manifold.operators.subquery    import SubQuery
from manifold.util.log              import Log 
from manifold.util.type             import returns

DUMPSTR_RENAME = "RENAME %r" 

#------------------------------------------------------------------
# RENAME node
#------------------------------------------------------------------

def do_rename(record, aliases):
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

    def collect(key, record):
        if isinstance(record, (list, Records)):
            # 1..N
            return [collect(key, r) for r in record]
        elif isinstance(record, Record):
            key_head, _, key_tail = key.partition(FIELD_SEPARATOR)
            return collect(key_tail, record[key_head])
            # 1..1
        else:
            assert not key, "Field not found"
            return record

    def handle_record(k, v, myrecord, data = None):
        """
        Convert the field name from k to v in myrecord. k and v will eventually
        have several dots.
        . cases when field length are not of same length are not handled
        """
        k_head, _, k_tail = k.partition(FIELD_SEPARATOR)
        v_head, _, v_tail = v.partition(FIELD_SEPARATOR)

        if not k_head in myrecord:
            return

        if k_tail and v_tail:

            if k_head != v_head:
                myrecord[v_head] = myrecord.pop(k_head)

            subrecord = myrecord[v_tail]

            if isinstance(subrecord, Records):
                # 1..N
                for _myrecord in subrecord:
                    handle_record(k_tail, v_tail, _myrecord)
            elif isinstance(subrecord, Record):
                # 1..1
                handle_record(k_tail, v_tail, subrecord)
            else:
                return

        elif not k_tail and not v_tail:
            # XXX Maybe such cases should never be reached.
            if k_head and k_head != v_head:
                myrecord[v_head] = myrecord.pop(k_head)
            else:
                myrecord[v_head] = data

        else:
            # We have either ktail or vtail"
            if k_tail: # and not v_tail
                # We will gather everything and put it in v_head
                myrecord[k_head] = collect(k_tail, myrecord[k_head])

            else: # v_tail and not k_tail
                # We have some data in subrecord, that needs to be affected to
                # some dictionaries whose key sequence is specified in v_tail.
                # This should allow a single level of indirection.
                # 
                # for example: users = [A, B, C]   =>    users = [{user_hrn: A}, {...}, {...}]
                data = myrecord[v_head]
                # eg. data = [A, B, C]

                if isinstance(data, Records):
                    raise Exception, "Not implemented"
                elif isinstance(data, Record):
                    raise Exception, "Not implemented"
                elif isinstance(data, list):
                    myrecord[v_head] = list()
                    for element in data:
                        myrecord[v_head].append({v_tail: element})
                else:
                    raise Exception, "Not implemented"

    for k, v in aliases.items():
        # Rename fields in place in the record
        handle_record(k, v, record)

    return record



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

#DEPRECATED|    def process_records(self, records):
#DEPRECATED|        """
#DEPRECATED|        This function replaces the list of records in the (query) packet.
#DEPRECATED|        """
#DEPRECATED|        return Records([self.process_record(r) for r in records])

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
            new_packet = do_rename(packet, self.get_aliases())
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
        return Projection(self, fields)

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
