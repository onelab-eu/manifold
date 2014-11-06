#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A QueryPlan manages an AST where each leaves are a From
# of a FromTable Operators.
#
# A From Node wraps a dummy Query having SELECT and WHERE
# clause homogenous to the query issued by the user.
# From Nodes are useful until having optimized the QueryPlan.
# Then, each Operator using a From Node is connected to a Socket
# plugged on the appropriate Interface.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import traceback
from types                          import StringTypes

from manifold.core.annotation       import Annotation
from manifold.core.capabilities     import Capabilities
from manifold.core.destination      import Destination
from manifold.core.field_names      import FieldNames
from manifold.core.key              import Key
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.record           import Record
from manifold.interfaces              import Interface
from manifold.operators.from_table  import FromTable
from manifold.operators.operator    import Operator
from manifold.operators.projection  import Projection
from manifold.operators.selection   import Selection
from manifold.operators.subquery    import SubQuery
from manifold.util.log              import Log
from manifold.util.type             import returns

class From(Operator, ChildSlotMixin):
    """
    From Operators are responsible to forward Packets between the AST
    and the involved Interfaces Nodes. It is mostly used during the
    AST optimization phase during which we need to support get_query()
    for each Operator of the AST.
    """

    #---------------------------------------------------------------------------
    # Constructors
    #---------------------------------------------------------------------------

    def __init__(self, interface, destination, capabilities, key, partitions = None):
        """
        Constructor.
        Args:
            destination: A Destination instance destinationing a Table provided by this Platform.
            annotation: An Annotation instance.
            capabilities: A Capabilities instance, set according to the metadata related
                to the Table queried by this From Node.
            key: A Key instance.
        """
        assert isinstance(interface, Interface),\
            "Invalid interface = %s (%s)" % (interface, type(interface))
        assert isinstance(destination, Destination),\
            "Invalid destination = %s (%s)" % (destination, type(destination))
        assert isinstance(capabilities, Capabilities),\
            "Invalid capabilities = %s (%s)" % (capabilities, type(capabilities))
        assert isinstance(key, Key),\
            "Invalid key = %s (%s)" % (key, type(key))

        Operator.__init__(self)
        ChildSlotMixin.__init__(self)

        self._interface      = interface
        self._destination  = destination
        self._capabilities = capabilities
        self._key          = key
        self._partitions   = partitions

    def copy(self):
        return From(self._interface, self._destination.copy(), self._capabilities, self._key)

    #---------------------------------------------------------------------------
    # 
    #---------------------------------------------------------------------------

    @returns(Interface)
    def get_interface(self):
        """
        Returns:
            The Interface instance used to query the Platform wrapped
            by this FROM node.
        """
        return self._interface

    @returns(StringTypes)
    def get_platform_name(self):
        """
        Returns:
            The name of the Platform queried by this FROM node.
        """
        return self.get_interface().get_platform_name()

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The name of the platform queried by this FROM node.
        """
        #return "%s" % self.get_query().to_sql(platform = self.get_platform_name())
        try:
            platform_name = self.get_platform_name()
            return "FROM %(namespace)s%(object_name)s [%(destination)s]" % {
                "namespace"  : "%s:" % platform_name if platform_name else "",
                "object_name" : self.get_destination().get_object_name(),
                "destination"      : self.get_destination()#.to_sql(platform = self.get_platform_name())
            }
        except Exception, e:
            print "Exception in repr: %s" % e

    @returns(Destination)
    def get_destination(self):
        """
        Returns:
            The Destination nested in this From instance.
        """
        return self._destination

    @returns(Capabilities)
    def get_capabilities(self):
        """
        Returns:
            The Capabilities nested in this From instance.
        """
        return self._capabilities

    @returns(bool)
    def has_children_with_fullquery(self):
        """
        Returns:
            True iif this Operator or at least one of its child uses
            fullquery Capabilities.
        """
        return self.get_capabilities().fullquery

    def send(self, packet, slot_id = None):
        """
        Process an incoming packet.
        Args:
            packet: A Packet instance.
        """

        # It is possible that a packet arrives with filters and fields while the gateways does not support them
        packet_field_names = packet.get_destination().get_field_names()
        packet_filter = packet.get_destination().get_filter()

        # XXX filters could be inserted by LEFT JOIN and SUBQUERY operators
        # while the platforms do not have the capabilities to understand them.
        # - What about doing this in the query plan construction, with empty
        # filters and field_names, so that it could be set by the query...
        # - How to treat fullquery ?

        target = self.copy() #From(self._interface, self._destination, self._capabilities, self._key)
        has_operators = False

        key_filter, remaining_filter = packet_filter.split_fields(self._key.get_field_names())
        if packet_filter:
            if not self.get_capabilities().selection:
                if self.get_capabilities().join:
                    if remaining_filter:
                        target = target.optimize_selection(remaining_filter)
                        target.format_downtree()
                        has_operators = True
                else:
                    target = target.optimize_selection(packet_filter)
                    target.format_downtree()
                    has_operators = True
        
        if packet_field_names and not packet_field_names.is_star() and not self.get_capabilities().projection:
            target = target.optimize_projection(packet_field_names)
            has_operators = True

        # The packet needs to come back in the operatorgraph, otherwise, it is
        # send directly to the receiving interface (or any other receiver that
        # will have been defined)

        if has_operators: 
            target.add_consumer(self)
            target.send(packet)
        else:
            # We need to add local filters to the query packet
            filter = self.get_destination().get_filter()
            packet.update_destination(lambda d: d.add_filter(filter))

            packet.set_receiver(self)
            self.get_interface().send(packet)

    @returns(Operator)
    def optimize_selection(self, filter):
        """
        Propagate a WHERE clause through this From instance.
        Args:
            filter: A Filter instance.
        Returns:
            The updated root Node of the sub-AST.
        """
        # First check, if the filter contradict the partitions, then we return None
        if self._partitions and not any(p & filter for p in self._partitions):
            return None

        # XXX Simplifications
        for predicate in filter:
            if predicate.get_field_names() == self._key.get_field_names() and predicate.has_empty_value():
                # The result of the request is empty, no need to instanciate any interface
                # Replace current node by an empty node
                consumers = self.get_consumers()
                self.clear_consumers() # XXX We must only unlink consumers involved in the QueryPlan that we're optimizing
                from_table = FromTable(self.get_destination(), list(), self._key)
#MANDO|                for consumer in consumers:
#MANDO|                    consumer.add_producer(from_table)
                Log.tmp("mando: a verifier")
                from_table.add_consumers(consumers)
#MANDO|
                Log.warning("From: optimize_selection: empty table")
                return from_table

        # Then, we decide how to insert the filter in the query plan, according to the platform capabilities

        if self.get_capabilities().selection:
            # Push filters into the From node
            self._destination.add_filter(filter)
            #old for predicate in filter:
            #old    self.query.filters.add(predicate)
            return self
        elif self.get_capabilities().join:
            key_filter, remaining_filter = filter.split_fields(self._key.get_field_names())

            if key_filter:
                # We push only parts related to the key (unless fullquery)
                if self.get_capabilities().fullquery:
                    self._destination.add_filter(filter)
                else:
                    self._destination.add_filter(key_filter)

            if remaining_filter:
                # FROM (= self) becomes the new producer for Selection
                selection = Selection(self, remaining_filter)
                Log.warning("We removed: selection.set_producer(self)")
                return selection

            else:
                return self

        else:
            # XXX New rework just like the previous elif XXX

            # Create a new Selection node
            selection = Selection(self, filter)

            if self.get_capabilities().fullquery:
                # We also push the filter down into the node
                for predicate in filter:
                    self._destination.add_filter(predicate)

            return selection

    @returns(Operator)
    def optimize_projection(self, fields):
        """
        Propagate a SELECT clause through this From instance.
        Args:
            fields: A set of String instances (queried fields).
        Returns:
            The updated root Node of the sub-AST.
        """
        # This should initially contain all fields provided by the table (and not *)
        provided_fields = self.get_destination().get_field_names()

        if self.get_capabilities().projection or self.get_capabilities().fullquery:
            self._destination.set_field_names(provided_fields & fields)

        if self.get_capabilities().projection:
            # Push fields into the From node
            return self
        else:
            # Provided fields is set to None if it corresponds to SELECT *

            # Test whether this From node can return every queried FieldNames.
            if provided_fields and not (fields <= provided_fields):
                Log.warning("From::optimize_projection: some requested fields (%s) are not provided by {%s} From node. Available fields are: {%s}" % (
                    ', '.join(list(fields - provided_fields)),
                    self.get_destination().get_object_name(),
                    ', '.join(list(provided_fields))
                ))

            # If this From node returns more FieldNames than those explicitely queried
            # (because the projection capability is not enabled), create an additional
            # Projection Node above this From Node in order to guarantee that
            # we only return queried fields
            if not provided_fields or provided_fields - fields:
                return Projection(self, fields)
                #projection.query = self.query.copy().filter_by(filter) # XXX
            return self

#DEPRECATED|    @returns(Operator)
#DEPRECATED|    def reorganize_create(self):
#DEPRECATED|        return self

    def subquery(self, ast, relation):
        # This is the only place where local subqueries are created. Here, we
        # need to inspect what is in the child to be sure no Rename or LeftJoin
        # is present under a local subquery
        # NOTE: add_child is also of concern
        return SubQuery.make(self, ast, relation)
