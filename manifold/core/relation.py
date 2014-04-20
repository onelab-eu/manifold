#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Class Relation:
# Represents relation between two Tables of the DBGraph.
# In this graph, arcs may store one or more Relation.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                      import StringTypes
from manifold.util.enum         import Enum
from manifold.util.predicate    import Predicate
from manifold.util.type         import returns, accepts

class Relation(object):

    types = Enum(
#UNUSED|        'COLLECTION',
        # Link 1..1 ------------------------------------------------------
        'SPECIALIZATION',      # p1::t is a SPECIALIZATION of {p1, p2}::t
        'PARENT',              # Inheritance: "vehicle" is a PARENT of "car"
        'CHILD',               # Inheritance: "car" is a CHILD of "vehicle"
        'LINK',                # Link 1..1 leading to a LeftJoin. Ex: "city" LJ "country" LJ "continent" (continent properties are also city properties)
        'LINK_11',             # Link 1..1 leading to a SubQuery. Ex: "traceroute.source" (source properties does not characterize a traceroute)
        # Link 1..N ------------------------------------------------------
        'LINK_1N',             # Link 1..N leading to a SubQuery, where parent table embeds IDs of children table.
        'LINK_1N_BACKWARDS',   # Link 1..N where children table embeds ID of parent table.
    )

    def __init__(self, type, predicate, name = None, local = False):
        """
        Constructor.
        Params:
            type: The type of Relation. See Relation.types.
                Example: Relation.types.LINK_11, Relation.types.PARENT...
            predicate: A Predicate instance
        """
        assert isinstance(predicate, Predicate), "Invalid predicate = %s (%s)" % (predicate, type(predicate))
        if name:
            assert isinstance(name, StringTypes), "Invalid name = %s (%s)" % (name, type(name))
        self.type       = type
        self.predicate  = predicate
        self.name       = name
        self._local     = local
#        if local:
#            print "RELATION", self, "IS LOCAL"

    def copy(self):
        return Relation(self.type, self.predicate.copy(), self.name, self._local)

    def get_type(self):
        """
        Returns:
            The type of Relation established. See Relation.types.
        """
        return self.type

    @returns(StringTypes)
    def get_str_type(self):
        """
        Returns:
            The type of Relation established (String format). See Relation.types.
        """
        return self.types.get_str(self.get_type())

    #@returns(Predicate)
    def get_predicate(self):
        """
        Returns:
            The Predicate instance related to this Relation.
        """
        return self.predicate

    @returns(bool)
    def is_local(self):
        return self._local

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The "%s" representation of this Relation.
        """
        return "<Relation<%s> %s: %s>" % (
            self.name if self.get_relation_name() else "",
            self.get_str_type(),
            self.get_predicate()
        )

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The "%r" representation of this Relation.
        """
        return "<%s, %s>" % (
            self.get_str_type(),
            self.name if self.get_relation_name() else ""
        )

    #@returns(StringTypes)
    def get_name(self):
        """
        Returns:
            A String instance or None. If this is a String, it contains
            the  name of the relation. This name is used to resolve a user Query
            For instance, if a Query involves "foo.bar", we expect to find
            a "bar" Relation stored in an out-edge of "foo" Table.
        """
        return self.name

    # DEPRECATED
    def get_relation_name(self):
        return self.get_name()

    @returns(bool)
    def requires_subquery(self):
        """
        Returns:
            True iif using this Relation implies to use a SubQuery Node
            in the QueryPlan. See manifold/operators/subquery.py.
        """
        return self.get_type() not in [Relation.types.LINK, Relation.types.CHILD, Relation.types.PARENT]

    @returns(bool)
    def requires_join(self):
        """
        Returns:
            True iif using this Relation implies to use a LeftJoin Node
            in the QueryPlan. See manifold/operators/left_join.py.
        """
        return not self.requires_subquery()
