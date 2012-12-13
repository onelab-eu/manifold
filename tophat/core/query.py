#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Query representation
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>

from types                  import StringTypes
from tophat.core.filter     import Filter, Predicate
from tophat.util.frozendict import frozendict
from copy                   import deepcopy

class ParameterError(StandardError): pass

class Query(object):
    """
    Implements a TopHat query.

    We assume this is a correct DAG specification.

    1/ A field designates several tables = OR specification.
    2/ The set of fields specifies a AND between OR clauses.
    """

    def __init__(self, *args, **kwargs):
        l = len(kwargs.keys())


        if len(args) == 1 and isinstance(args[0], Query):
            # Copy
            return deepcopy(args[0])

        # Initialization from a tuple
        if len(args) in range(2, 7) and type(args) == tuple:
            # Note: range(x,y) <=> [x, y[
            self.action, self.fact_table, self.filters, self.params, self.fields, self.ts = args

        # Initialization from a dict (action & fact_table are mandatory)
        elif 'fact_table' in kwargs:
            if 'action' in kwargs:
                self.action = kwargs['action']
                del kwargs['action']
            else:
                print "W: defaulting to get action"
                self.action = 'get'

            self.fact_table = kwargs['fact_table']
            del kwargs['fact_table']

            if 'filters' in kwargs:
                self.filters = kwargs['filters']
                del kwargs['filters']
            else:
                self.filters = Filter([])

            if 'fields' in kwargs:
                self.fields = set(kwargs['fields'])
                del kwargs['fields']
            else:
                self.fields = set([])

            # "update table set x = 3" => params == set
            if 'params' in kwargs:
                self.params = kwargs['params']
                del kwargs['params']
            else:
                self.params = {}

            if 'ts' in kwargs:
                self.ts = kwargs['ts']
                del kwargs['ts']
            else:
                self.ts = 'now' 

            if kwargs:
                raise ParameterError, "Invalid parameter(s) : %r" % kwargs.keys()
                return
        else:
                raise ParameterError, "No valid constructor found for %s : args=%r" % (self.__class__.__name__, args)

        if not self.filters: self.filters = Filter([])
        if not self.params:  self.params  = {}
        if not self.fields:  self.fields  = set([])
        if not self.ts:      self.ts      = "now" 

        if isinstance(self.filters, list):
            f = self.filters
            self.filters = Filter([])
            for x in f:
                pred = Predicate(x)
                self.filters.add(pred)

        if isinstance(self.fields, list):
            self.fields = set(self.fields)

        for field in self.fields:
            if not isinstance(field, StringTypes):
                raise TypeError("Invalid field name %s (string expected, got %s)" % (field, type(field)))

    def __str__(self):
        return "SELECT %s FROM %s WHERE %s" % (', '.join(self.fields), self.fact_table, self.filters)

    def __key(self):
        return (self.action, self.fact_table, self.filters, frozendict(self.params), frozenset(self.fields))

    def __hash__(self):
        return hash(self.__key())

