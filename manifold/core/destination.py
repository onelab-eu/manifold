#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Destination corresponds to an entry in the routing table of a
# Manifold router (basically this a view over a given object).
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                      import StringTypes
from manifold.core.filter       import Filter
from manifold.core.field_names  import FieldNames
from manifold.util.misc         import is_iterable
from manifold.util.predicate    import Predicate
from manifold.util.type        import accepts, returns

import copy

class Destination(object):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, object = None, filter = None, fields_names = None):
        """
        Constructor.
        Args:
            object: A String corresponding to a Table Name or None.
            filter: A Filter instance or None.
            fields_names: A FieldNames or None.
        """
        # Until all fields_names have the proper type...
        if fields_names is None:
            fields_names = FieldNames(star = False)
        elif not isinstance(fields_names, FieldNames):
            fields_names = FieldNames(fields_names)

        self._object = object 
        self._filter = filter if filter else Filter()   # Partition
        self._field_names = fields_names if fields_names else FieldNames()   # Hyperplan

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def get_object(self):
        """
        Returns:
            The table name corresponding (None if unset).
        """
        return self._object

    def set_object(self, object):
        """
        Args:
            object: A String corresponding to the table name.
        """
        assert isinstance(object, StringTypes)
        self._object = object

    @returns(Filter)
    def get_filter(self):
        """
        Returns:
            The Filter instance corresponding to this Destination (None if unset).
        """
        return self._filter

    def add_filter(self, predicate_or_filter):
        """
        Args:
            predicate_or_filter: A Predicate or a Filter instance.
        """
        assert isinstance(predicate_or_filter, Predicate) or isinstance(predicate_or_filter, Filter)
        self._filter.add(predicate_or_filter)

    @returns(FieldNames)
    def get_field_names(self):
        """
        Returns:
            The FieldNames instance corresponding to this Destination (None if unset).
        """
        return self._field_names

#UNUSED|    def add_fields_names(self, fields_names):
#UNUSED|        """
#UNUSED|        Args:
#UNUSED|            fields_names: A FieldNames instance.
#UNUSED|        """
#UNUSED|        if is_iterable(fields_names):
#UNUSED|            map(self.add_fields_names, fields_names)
#UNUSED|            return
#UNUSED|
#UNUSED|        if fields_names and self._field_names is not None:
#UNUSED|            self._field_names.add(fields_names)

    #---------------------------------------------------------------------------
    # str repr
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Query.
        """
        return self.__repr__()

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Query.
        """
        return "%r" % ((self._object, self._filter, self._field_names), )

    #---------------------------------------------------------------------------
    # Algebra of operators
    #---------------------------------------------------------------------------

    # XXX This should be distributed in the different operators

    #@returns(Destination)
    def left_join(self, destination): 
        """
        Combine this Destination and another one according to an operator
        according to the LeftJoin Operator.
        Args:
            destination: A Destination instance.
        Returns:
            The corresponding Destination.
        """
        return Destination(
            object = self._object,
            filter = self._filter | destination.get_filter(),
            fields_names = self._field_names | destination.get_field_names()
        )

    #@returns(Destination)
    def right_join(self, destination):
        """
        Combine this Destination and another one according to an operator
        according to the RightJoin Operator.
        Args:
            destination: A Destination instance.
        Returns:
            The corresponding Destination.
        """
        return Destination(
            object = destination._object,
            filter = self._filter | destination.get_filter(),
            fields_names = self._field_names | destination.get_field_names()
        )

    #@returns(Destination)
    def selection(self, filter):
        """
        Combine this Destination and another one according to an operator
        according to the Selection Operator.
        Args:
            filter: A Filter instance.
        Returns:
            The corresponding Destination.
        """
        return Destination(
            object = self._object,
            filter = self._filter & filter,
            fields_names = self._field_names
        )

    #@returns(Destination)
    def projection(self, field_names):
        """
        Combine this Destination and another one according to an operator
        according to the Projection Operator.
        Args:
            field_names: A FieldNames instance.
        Returns:
            The corresponding Destination.
        """
        return Destination(
            object = self._object,
            filter = self._filter,
            fields_names = self._field_names & field_names
        )

    #@returns(Destination)
    def rename(self, aliases):
        """
        Combine this Destination and another one according to an operator
        according to the Rename Operator.
        Args:
            aliases: A {String : String} mapping the old field name and
                the new field name. 
        Returns:
            The corresponding Destination.
        """
        return Destination(
            object = self._object,
            filter = self._filter.copy().rename(aliases),
            fields_names = self._field_names.copy().rename(aliases)
        )

    # XXX This is the opposite of split, we name it merge
    #@returns(Destination)
    def subquery(self, children_destination_relation_list):
        """
        Combine this Destination and another one according to an operator
        according to the SubQuery Operator.
        Args:
            children_destination_relation_list:
        Returns:
            The corresponding Destination.
        """
        object = self._object
        filter = self._filter
        field_names = self._field_names
        for destination, relation in children_destination_relation_list:
            # We are sure that all relations triggering a SubQuery have a name
            name = relation.get_relation_name()
            for predicate in destination.get_filter():
                key, op, value = predicate.get_tuple()
                filter.filter_by("%s.%s" % (name, key), op, value)
            field_names |= FieldNames(['%s.%s' % (name, f) for f in destination.get_field_names()])
        return Destination(object, filter, field_names)
