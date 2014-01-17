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
    # Algebra of operators
    #---------------------------------------------------------------------------

    def left_join(self, destination): 
        return Destination(
            object = self.get_object(),
            filter = self.filters | destination.filters,
            fields = self.fields  | destination.fields)

    def selection(self, filter):
        return Destination(
            object = self._object(),
            filter = self._filter & filter,
            fields = self._fields)

    def projection(self, fields):
        return Destination(
            object = self._object(),
            filter = self._filter,
            fields = self._fields) # equivalent to (self._fields & fields) since (self._fields c fields)
