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
from manifold.util.types        import accepts, returns

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
            The table name corresponding 
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
        return self._filter

    def add_filter(self, predicate_or_filter):
        self._filter.add(predicate_or_filter)

    def get_field_names(self):
        return self._field_names

#DEPRECATED|    def add_fields_names(self, fields_names):
#DEPRECATED|        """
#DEPRECATED|        Args:
#DEPRECATED|            fields_names: A FieldNames instance.
#DEPRECATED|        """
#DEPRECATED|        if is_iterable(fields_names):
#DEPRECATED|            map(self.add_fields_names, fields_names)
#DEPRECATED|            return
#DEPRECATED|
#DEPRECATED|        if fields_names and self._field_names is not None:
#DEPRECATED|            self._field_names.add(fields_names)

    #---------------------------------------------------------------------------
    # str repr
    #---------------------------------------------------------------------------

    def __str__(self):
        """
        Returns:
            The '%s' representation of this Query.
        """
        return self.__repr__()

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

    def left_join(self, destination): 
        return Destination(
            object = self._object,
            filter = self._filter | destination.get_filter(),
            fields_names = self._field_names | destination.get_field_names()
        )

    def right_join(self, destination):
        return Destination(
            object = destination._object,
            filter = self._filter | destination.get_filter(),
            fields_names = self._field_names | destination.get_field_names()
        )

    def selection(self, filter):
        return Destination(
            object = self._object,
            filter = self._filter & filter,
            fields_names = self._field_names
        )

    def projection(self, field_names):
        return Destination(
            object = self._object,
            filter = self._filter,
            fields_names = self._field_names & field_names
        )

    def rename(self, aliases):
        return Destination(
            object = self._object,
            filter = self._filter.copy().rename(aliases),
            fields_names = self._field_names.copy().rename(aliases)
        )

    # XXX This is the opposite of split, we name it merge
    def subquery(self, children_destination_relation_list):
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
