#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Filter is a set of Predicates. It is used to
# model a Query and a Selection. See also:
#
#   manifold/core/query.py
#   manifold/operators/selection.py
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import copy
from types                      import StringTypes

from manifold.core.field_names  import FieldNames
from manifold.util.misc         import is_iterable
from manifold.util.predicate    import Predicate, eq, included
from manifold.util.type         import accepts, returns 

class Filter(set):
    """
    A Filter is a set of Predicate instances
    """

    #def __init__(self, s=()):
    #    super(Filter, self).__init__(s)

    @staticmethod
    #@returns(Filter)
    def from_list(l):
        """
        Create a Filter instance by using an input list.
        Args:
            l: A list of Predicate instances.
        """
        f = Filter()
        try:
            for element in l:
                f.add(Predicate(*element))
        except Exception, e:
            print "Error in setting Filter from list", e
            return None
        return f
        
    @staticmethod
    #@returns(Filter)
    def from_dict(d):
        """
        Create a Filter instance by using an input dict.
        Args:
            d: A dict {key : value} instance where each
               key-value pair leads to a Predicate.
               'key' could start with the operator to be
               used in the predicate, otherwise we use
               '=' by default.
        """
        f = Filter()
        for key, value in d.items():
            if key[0] in Predicate.operators.keys():
                f.add(Predicate(key[1:], key[0], value))
            else:
                f.add(Predicate(key, '=', value))
        return f

    @returns(list)
    def to_list(self):
        """
        Returns:
            The list corresponding to this Filter instance.
        """
        ret = list() 
        for predicate in self:
            ret.append(predicate.to_list())
        return ret

    @staticmethod
    def from_clause(clause):
        """
        NOTE: We can only handle simple clauses formed of AND fields.
        """
        raise Exception, "Not implemented"

    @staticmethod
    def from_string(string):
        """
        """
        from manifold.input.sql import SQLParser
        p = SQLParser()
        ret = p.filter.parseString(string, parseAll=True)
        return ret[0] if ret else None

    #@returns(Filter)
    def filter_by(self, predicate):
        """
        Update this Filter by adding a Predicate.
        Args:
            predicate: A Predicate instance.
        Returns:
            The resulting Filter instance.
        """
        assert isinstance(predicate, Predicate),\
            "Invalid predicate = %s (%s)" % (predicate, type(predicate))
        self.add(predicate)
        return self

    def add(self, predicate_or_filter):
        """
        Adds a predicate or a filter (a set of predicate) -- or a list thereof -- to the current filter.
        """
        if is_iterable(predicate_or_filter):
            map(self.add, predicate_or_filter)
            return

        assert isinstance(predicate_or_filter, Predicate)
        set.add(self, predicate_or_filter)

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Filter.
        """
        if len(self) > 0:
            return ' AND '.join([str(pred) for pred in self])
        else:
            return '<empty filter>'

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Filter.
        """
        return '<Filter: %s>' % self 

    @returns(tuple)
    def __key(self):
        return tuple([hash(pred) for pred in self])

    def __hash__(self):
        return hash(self.__key())

    def __additem__(self, value):
        if not isinstance(value, Predicate):
            raise TypeError("Element of class Predicate expected, received %s" % value.__class__.__name__)
        set.__additem__(self, value)


    def copy(self):
        return copy.deepcopy(self)

    @returns(set)
    def keys(self):
        """
        Returns:
            A set of String corresponding to each field name
            involved in this Filter.
        """
        return set([x.key for x in self])

    # XXX THESE FUNCTIONS SHOULD ACCEPT MULTIPLE FIELD NAMES

    @returns(bool)
    def has(self, key):
        for x in self:
            if x.key == key:
                return True
        return False

    @returns(bool)
    def has_op(self, key, op):
        for x in self:
            if x.key == key and x.op == op:
                return True
        return False

    @returns(bool)
    def has_eq(self, key):
        return self.has_op(key, eq)

    def get(self, key):
        ret = []
        for x in self:
            if x.key == key:
                ret.append(x)
        return ret

    def delete(self, key):
        to_del = []
        for x in self:
            if x.key == key:
                to_del.append(x)
        for x in to_del:
            self.remove(x)
            
        #self = filter(lambda x: x.key != key, self)

    def get_op(self, key, op):
        if isinstance(op, (list, tuple, set)):
            for x in self:
                if x.key == key and x.op in op:
                    return x.value
        else:
            for x in self:
                if x.key == key and x.op == op:
                    return x.value
        return None

    def get_eq(self, key):
        return self.get_op(key, eq)

    def set_op(self, key, op, value):
        for x in self:
            if x.key == key and x.op == op:
                x.value = value
                return
        raise KeyError, key

    def set_eq(self, key, value):
        return self.set_op(key, eq, value)

    def get_predicates(self, key):
        # XXX Would deserve returning a filter (cf usage in SFA gateway)
        ret = []
        for x in self:
            if x.key == key:
                ret.append(x)
        return ret

#    def filter(self, dic):
#        # We go through every filter sequentially
#        for predicate in self:
#            print "predicate", predicate
#            dic = predicate.filter(dic)
#        return dic

    @returns(bool)
    def match(self, dic, ignore_missing=True):
        for predicate in self:
            if not predicate.match(dic, ignore_missing):
                return False
        return True

    @returns(list)
    def filter(self, l):
        output = []
        for x in l:
            if self.match(x):
                output.append(x)
        return output

    @returns(FieldNames)
    def get_field_names(self):
        field_names = FieldNames()
        for predicate in self:
            field_names |= predicate.get_field_names()
        return field_names

    #@returns(Filter)
    def grep(self, fun):
        return Filter([x for x in self if fun(x)])

    #@returns(Filter)
    def rgrep(self, fun):
        return Filter([x for x in self if not fun(x)])

    #@returns(tuple)
    #@returns(Filter)
    def split(self, fun, true_only = False):
        true_filter, false_filter = Filter(), Filter()
        for predicate in self:
            if fun(predicate):
                true_filter.add(predicate)
            else:
                false_filter.add(predicate)
        if true_only:
            return true_filter
        else:
            return (true_filter, false_filter)
        

    def split_fields(self, fields, true_only = False):
        return self.split(lambda predicate: predicate.get_key() in fields, true_only)

    def provides_key_field(self, key_fields):
        # No support for tuples
        for field in key_fields:
            if not self.has_op(field, eq) and not self.has_op(field, included):
                print "missing key fields", field, "in query filters", self
                return False
        return True

    def rename(self, aliases):
        for predicate in self:
            predicate.rename(aliases)
        return self

    def get_field_values(self, field):
        """
        This function returns the values that are determined by the filters for
        a given field, or None is the filter is not *setting* determined values.
        """
        value_list = list()
        for predicate in self:
            key, op, value = predicate.get_tuple()

            if key == field:
                extract_tuple = False
            elif key == (field, ):
                extract_tuple = True
            else:
                continue

            if op == eq:
                if extract_tuple:
                    value = value[0]
                value_list.append(value)
            elif op == included:
                if extract_tuple:
                    value = [x[0] for x in value]
                value_list.extend(value)
            else:
                continue

        return list(set(value_list))

    def __and__(self, other):
        s = self.copy()
        for o_predicate in other:
            o_key, o_op, o_value = o_predicate.get_tuple()
            for predicate in self:
                key, op, value = predicate.get_tuple()
                if key != o_key:
                    continue

                if op == eq:
                    if o_op == eq:
                        if value != o_value:
                            return None
                    elif o_op == included:
                        if value not in o_value:
                            return None
                elif op == included:
                    if o_op == eq:
                        if o_value not in value:
                            return None
                    elif o_op == included:
                        if not set(o_value) & set(value):
                            print "set(o_value)", set(o_value)
                            print "set(value)", set(value)
                            print "set(o_value) & set(value)", set(o_value) & set(value)
                            return None
            
            # No conflict found, we can add the predicate to s
            s.add(o_predicate) 

        print self, "*and*", other, "==", s
            
        return s
