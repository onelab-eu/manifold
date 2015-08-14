#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# QueryFactory creates a Query instance from a packet or from a dict
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Loïc Baron          <loic.baron@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>
#   Jordan Augé         <jordan.auge@lip6.fr>

from manifold.core.clause       import Clause
from manifold.core.field_names  import FieldNames
from manifold.core.query        import Query
from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

class QueryFactory(object):
    """
    QueryFactory creates a Query instance
    """
    @staticmethod
    @returns(Query)
    def from_dict(query_dict):
        """
        Build a Query according to an input dict.
        Args:
            query_dict: A dict instance. Supported keys:
                "action",
                "filters",
                "fields",
                "timestamp",
                "variable"
        """
        assert isinstance(query_dict, dict)
        query = Query()
        if "action" in query_dict:
            query.action = query_dict["action"]
            del query_dict["action"]
        else:
            Log.warning("query: Defaulting to get action")
            query.action = ACTION_GET


        query.object = query_dict["object"]
        del query_dict["object"]

        if "filters" in query_dict:
            query.filters = query_dict["filters"]
            del query_dict["filters"]
        else:
            query.filters = Clause()

        if "fields" in query_dict:
            # '*' Handling
            fields = query_dict.pop('fields')
            if '*' in fields:
                #Log.tmp("SET STAR IN KWARGS")
                query.fields = FieldNames(star = True)
            else:
                query.fields = FieldNames(fields, star = False)
        else:
            query.fields = FieldNames(star = True)

        # "update table set x = 3" => params == set
        if "params" in query_dict:
            query.params = query_dict["params"]
            del query_dict["params"]
        else:
            query.params = {}

        if "timestamp" in query_dict:
            query.timestamp = query_dict["timestamp"]
            del query_dict["timestamp"]
        else:
            query.timestamp = "now"

        # Indicates the key used to store the result of this query in manifold-shell
        # Ex: $myVariable = SELECT foo FROM bar
        if "variable" in query_dict:
            query.variable = query_dict["variable"][0]
            del query_dict["variable"]
        else:
            query.variable = None

        query.sanitize()
        return query

    @staticmethod
    def from_packet(packet):
        Log.warning("Distinction between params.keys() and fields is not so good")
        destination = packet.get_destination()

        object_name = destination.get_object_name()
        namespace   = destination.get_namespace()
        filters     = destination.get_filter()
        field_names = destination.get_field_names()

        # TODO: action
        params      = packet.get_data()

        if namespace:
            query = Query.get("%s:%s" % (namespace, object_name))
        else:
            query = Query.get(object_name)
        if filters:
            query.filter_by(filters)
        if field_names:
            query.select(field_names)
        if params:
            query.set(params)
        return query


