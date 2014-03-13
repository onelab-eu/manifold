# -*- coding: utf-8 -*-

from types                          import StringTypes
from copy                           import copy

# The distinction between parent and children fields is based on the
# Fields.FIELD_SEPARATOR character.
#
# See manifold.operators.subquery
FIELD_SEPARATOR = '.'

class Fields(set):
    """
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, *args, **kwargs):
        star = kwargs.pop('star', True)
        set.__init__(self, *args, **kwargs)
        self._star = star

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def is_star(self):
        return self._star

    def set_star(self):
        self._star = True
        self.clear()

    def unset_star(self, fields = None):
        self._star = False
        if fields:
            self |= fields

    def is_empty(self):
        return not self.is_star() and not self

    #---------------------------------------------------------------------------
    # Iterators
    #---------------------------------------------------------------------------

    def iter_field_subfield(self):
        for f in self:
            field, _, subfield = f.partition(FIELD_SEPARATOR)
            yield (field, subfield)

    #---------------------------------------------------------------------------
    # Overloaded set internal functions
    #---------------------------------------------------------------------------

    def __or__(self, fields):
        if self.is_star() or fields.is_star():
            return Fields(star = True)
        else:
            return set.__or__(self, fields)

    def __ior__(self, fields):
        if self.is_star() or fields.is_star():
            self.set_star()
        else:
            return set.__ior__(self, fields)

    def __and__(self, fields):
        if self.is_star():
            return fields.copy()
        elif fields.is_star():
            return self.copy()
        else:
            return set.__and__(self, fields)

    def __iand__(self, fields):
        if self.is_star():
            self.unset_star(fields)
        elif fields.is_star():
            pass
        else:
            set.__iand__(self, fields)

    #---------------------------------------------------------------------------
    # Overloaded set comparison functions
    #---------------------------------------------------------------------------

    def __eq__(self, other):
        return self.is_star() and other.is_star() or set.__eq__(self, other)

    def __le__(self, other):
        return self.is_star() and other.is_star() or set.__eq__(self, other)

    # Defined with respect of previous functions

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self <= other and self != other

    def __ge__(self, other):
        return other <= self

    def __gt__(self, other):
        return other < self

    #---------------------------------------------------------------------------
    # Overloaded set functions
    #---------------------------------------------------------------------------

    def add(self, field_name):
        if not isinstance(field, StringTypes):
            raise TypeError("Invalid field name %s (string expected, got %s)" % (field, type(field)))

        if not self.is_star():
            set.add(self, field_name)
        
    def clear(self):
        self._star = False
        set.clear(self)
