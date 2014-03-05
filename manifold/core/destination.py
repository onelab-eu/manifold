# -*- coding: utf-8 -*-

class Destination(object):


    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, object, filter, fields):
        self._object = object 
        self._filter = filter # Partition
        self._fields = fields # Hyperplan

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_object(self):
        return self._object

    def get_filter(self):
        return self._filter

    def get_fields(self):
        return self._fields

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
