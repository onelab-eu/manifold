#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Query representation
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>
#   Thierry Parmentelat <thierry.parmentelat@inria.fr>

import json
from types                      import StringTypes

from manifold.core.query        import Query
from manifold.util.log          import Log
from manifold.util.type         import returns, accepts


# TODO Factorize with Query

class AnalyzedQuery(Query):

    # XXX we might need to propagate special parameters sur as DEBUG, etc.

    def __init__(self, query = None, metadata = None):
        """
        Constructor.
        Args:
            query: A Query instance.
            metadata: A DBGraph instance.
        """
        self.clear()
        self.metadata = metadata
        if query:
            self.query_uuid = query.query_uuid
            self.analyze(query)
        else:
            self.query_uuid = uniqid()

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this AnalyzedQuery.
        """
        out = list()
        fields = self.get_select()
        fields = "*" if fields.is_star() else ", ".join([for field in fields])
        out.append("SELECT %s FROM %s WHERE %s" % (
            fields,
            self.get_from(),
            self.get_where()
        ))
        cpt = 1
        for method, subquery in self.subqueries():
            out.append('  [SQ #%d : %s] %s' % (cpt, method, str(subquery)))
            cpt += 1

        return "\n".join(out)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this AnalyzedQuery.
        """
        return str(self)

    def clear(self):
        """
        Clear all the attributes of this AnalyzedQuery.
        """
        super(AnalyzedQuery, self).clear()
        self._subqueries = dict()

    def subquery(self, method):
        # Allows for the construction of a subquery
        if not method in self._subqueries:
            analyzed_query = AnalyzedQuery(metadata = self.metadata)
            analyzed_query.action = self.get_action()
            try:
                type = self.metadata.get_field_type(self.get_from(), method)
            except ValueError ,e: # backwards 1..N
                type = method
            analyzed_query.object = type
            self._subqueries[method] = analyzed_query
        return self._subqueries[method]

    def get_subquery(self, method):
        return self._subqueries.get(method, None)

    def remove_subquery(self, method):
        del self._subqueries[method]

    def get_subquery_names(self):
        return set(self._subqueries.keys())

    def get_subqueries(self):
        return self._subqueries

    def subqueries(self):
        """
        Iterates on the subqueries related to this AnalyzedQuery.
        """
        for method, subquery in self._subqueries.iteritems():
            yield (method, subquery)

    #@returns(AnalyzedQuery)
    def filter_by(self, filters):
        """
        Update this AnalyzedQuery to complete its Filter.
        Args:
            filters: A set, list, tuple or Filter instance.
        Returns:
            The updated AnalyzedQuery.
        """
        if not isinstance(filters, (set, list, tuple, Filter)):
            filters = [filters]
        for predicate in filters:
            if predicate and '.' in predicate.key:
                method, subkey = predicate.key.split('.', 1)
                # Method contains the name of the subquery, we need the type
                # XXX type = self.metadata.get_field_type(self.object, method)
                sub_pred = Predicate(subkey, predicate.op, predicate.value)
                self.subquery(method).filter_by(sub_pred)
            else:
                super(AnalyzedQuery, self).filter_by(predicate)
        return self

    #@returns(AnalyzedQuery)
    def select(self, *fields):

        # XXX passing None should reset fields in all subqueries

        # Accept passing iterables
        if len(fields) == 1:
            tmp, = fields
            if isinstance(tmp, (list, tuple, set, frozenset)):
                fields = tuple(tmp)

        for field in fields:
            if field and '.' in field:
                method, subfield = field.split('.', 1)
                # Method contains the name of the subquery, we need the type
                # XXX type = self.metadata.get_field_type(self.object, method)
                self.subquery(method).select(subfield)
            else:
                super(AnalyzedQuery, self).select(field)
        return self

    #@returns(AnalyzedQuery)
    def set(self, params):
        for param, value in self.params.items():
            if '.' in param:
                method, subparam = param.split('.', 1)
                # Method contains the name of the subquery, we need the type
                # XXX type = self.metadata.get_field_type(self.object, method)
                self.subquery(method).set({subparam: value})
            else:
                super(AnalyzedQuery, self).set({param: value})
        return self

    def analyze(self, query):
        """
        Update this AnalyzedQuery according to an input Query.
        Args:
            query: A Query instance.
        """
        self.clear()
        self.action = query.get_action()
        self.object = query.get_from()
        self.filter_by(query.get_where())
        self.set(query.get_params())
        self.select(query.get_select())

    @returns(StringTypes)
    def to_json(self):
        """
        Returns:
            The json String corresponding to this AnalyzedQuery.
        """
        query_uuid = self.query_uuid
        a = self.action
        o = self.object
        t = self.timestamp
        f = json.dumps(self.filters.to_list())
        p = json.dumps(self.params)
        c = json.dumps(list(self.fields))
        # xxx unique can be removed, but for now we pad the js structure
        unique = 0

        aq = "null"
        sq = ", ".join(["'%s':%s" % (object, subquery.to_json()) for (object, subquery) in self._subqueries.iteritems()])
        sq = "{%s}" % sq

        result = " new ManifoldQuery('%(a)s', '%(o)s', '%(t)s', %(f)s, %(p)s, %(c)s, %(unique)s, '%(query_uuid)s', %(aq)s, %(sq)s)" %locals()
        Log.debug('ManifoldQuery.to_json: %s' % result)
        return result
