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
from manifold.util.log          import Log
from manifold.util.misc         import is_iterable
from manifold.util.predicate    import Predicate
from manifold.util.type         import accepts, returns

import copy

class Destination(object):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, object = None, filter = None, field_names = None, origin = None, namespace = None):
        """
        Constructor.
        Args:
            object: A String corresponding to a Table Name or None.
            filter: A Filter instance or None.
            field_names: A FieldNames or None.
        """
        # Until all field_names have the proper type...
        if field_names is None:
            field_names = FieldNames(star = False)
        elif not isinstance(field_names, FieldNames):
            field_names = FieldNames(field_names)

        self._object_name = object 
        self._filter = filter if filter else Filter()   # Partition
        self._field_names = field_names if field_names else FieldNames(star=True)   # Hyperplan
        self._origin = origin
        self._namespace = namespace

    def copy(self):
        return copy.deepcopy(self)

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def get_object_name(self):
        """
        Returns:
            The table name corresponding (None if unset).
        """
        return self._object_name

    # DEPRECATED
    def get_object(self):
        Log.warning("get_object is deprecated")
        return self.get_object_name()

    def set_object(self, object):
        """
        Args:
            object: A String corresponding to the table name.
        """
        assert isinstance(object, StringTypes)
        self._object_name = object

    def get_namespace(self):
        return self._namespace

    def set_namespace(self, namespace):
        self._namespace = namespace
        return self

    def clear_namespace(self):
        self._namespace = None
        return self

    def get_origin(self):
        return self._origin

    def set_origin(self, origin):
        self._origin = origin

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
        return self

    @returns(FieldNames)
    def get_field_names(self):
        """
        Returns:
            The FieldNames instance corresponding to this Destination (None if unset).
        """
        return self._field_names

    def add_field_names(self, field_names):
        """
        Args:
            field_names: A FieldNames instance.
        """
        if is_iterable(field_names):
            map(self.add_field_names, field_names)
            return

        if field_names and self._field_names is not None:
            self._field_names.add(field_names)
        return self

    def set_field_names(self, field_names):
        self._field_names = field_names
        return self

    def __eq__(self, other):
        return self.get_object() == other.get_object() and \
                self.get_filter() == other.get_filter() and \
                self.get_field_names() == other.get_field_names()

    def __hash__(self):
        return hash((self.get_object(), self.get_filter(), self.get_field_names()))

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
        return "%r" % ((self._namespace, self._object_name, self._filter, self._field_names), )

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
            object = self._object_name,
            filter = self._filter | destination.get_filter(),
            field_names = self._field_names | destination.get_field_names(),
            origin = self._origin
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
            field_names = self._field_names | destination.get_field_names(),
            origin = self._origin
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
            object = self._object_name,
            filter = self._filter & filter,
            field_names = self._field_names,
            origin = self._origin
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
            object = self._object_name,
            filter = self._filter,
            field_names = self._field_names & field_names,
            origin = self._origin
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
            object = self._object_name,
            filter = self._filter.copy().rename(aliases),
            field_names = self._field_names.copy().rename(aliases),
            origin = self._origin
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
        object = self._object_name
        filter = self._filter
        field_names = self._field_names
        for destination, relation in children_destination_relation_list:
            # We are sure that all relations triggering a SubQuery have a name
            name = relation.get_relation_name()
            for predicate in destination.get_filter():
                key, op, value = predicate.get_tuple()
                filter.filter_by("%s.%s" % (name, key), op, value)
            field_names |= FieldNames(['%s.%s' % (name, f) for f in destination.get_field_names()])
        origin = self._origin
        return Destination(object, filter, field_names, origin)
