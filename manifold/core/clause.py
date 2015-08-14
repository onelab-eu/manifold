#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Clause will become a tree of Filters with logical operators. 
# For the moment it is only a Filter handling and operators.
# It is used to define an Address.
# See also:
#
#   manifold/core/filter.py
#   manifold/core/address.py
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Loïc Baron          <loic.baron@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from manifold.core.filter   import Filter as Clause

#class Clause:
#    """
#    A Clause will become a tree of Filters with logical operators
#    """
#    def __init__(self):
#        self._data = None
#
#    @staticmethod
#    def from_filter(self, filter):
#        """
#        This method will disappear once we will be able to build
#        a tree of Predicate.
#        """
#        assert isinstance(filter, Filter)
#        ret = Clause()
#        ret._data = filter
#        return ret
#
#    def __iand__(self, predicate):
#        assert isinstance(self._data, Filter) # temporary assert
#        assert isinstance(predicate, Predicate)
#        self._data.add(predicate)
#
#    def __ior__(self, predicate):
#        """
#        For the moment we can only handle simple clauses formed of AND fields.
#        """
#        assert isinstance(self._data, Filter) #temporary assert
#        assert isinstance(predicate, Predicate)
#        raise Exception, "Not implemented"
#
