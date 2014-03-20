# -*- coding: utf-8 -*-

from manifold.core.filter import Filter
from manifold.core.fields import Fields
from manifold.util.misc   import is_iterable

class Destination(object):


    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, object = None, filter = None, fields = None):
        # Until all fields have the proper type...
        if fields is None:
            fields = Fields(star = True)
        elif not isinstance(fields, Fields):
            fields = Fields(fields)

        self._object = object 
        self._filter = filter if filter else Filter()   # Partition
        self._fields = fields if fields else Fields()   # Hyperplan


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_object(self):
        return self._object

    def set_object(self, object):
        self._object = object

    def get_filter(self):
        return self._filter

    def add_filter(self, predicate_or_filter):
        self._filter.add(predicate_or_filter)

    def get_fields(self):
        return self._fields

    def add_fields(self, fields):
        if is_iterable(fields):
            map(self.add_fields, fields)
            return

        if self._fields is not None:
            self._fields.add(fields)

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
        return "%r" % ((self._object, self._filter, self._fields), )

    #---------------------------------------------------------------------------
    # Algebra of operators
    #---------------------------------------------------------------------------

    # XXX This should be distributed in the different operators

    def left_join(self, destination): 
        return Destination(
            object = self._object,
            filter = self._filter | destination.get_filter(),
            fields = self._fields | destination.get_fields())

    def right_join(self, destination):
        return Destination(
            object = destination._object,
            filter = self._filter | destination.get_filter(),
            fields = self._fields | destination.get_fields())

    def selection(self, filter):
        return Destination(
            object = self._object,
            filter = self._filter & filter,
            fields = self._fields)

    def projection(self, fields):
        return Destination(
            object = self._object,
            filter = self._filter,
            fields = self._fields & fields)

    # XXX This is the opposite of split, we name it merge
    def subquery(self, children_destination_relation_list):
        object = self._object
        filter = self._filter
        fields = self._fields
        for destination, relation in children_destination_relation_list:
            # We are sure that all relations triggering a SubQuery have a name
            name = relation.get_name()
            for predicate in destination.get_filter():
                key, op, value = predicate.get_tuple()
                filter.filter_by("%s.%s" % (name, key), op, value)
            fields |= set(['%s.%s' % (name, f) for f in destination.get_fields()])
