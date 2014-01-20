#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Capabilities corresponds to operations supported by a Table
# announced by a Platform.
# http://trac.myslice.info/wiki/Manifold/Extensions/Gateways
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                        import StringTypes
from manifold.util.type           import returns, accepts

class Capabilities(object):
    
    KEYS = [
        # Manifold operators # SQL equivalent
        "retrieve",          # FROM
        "join",              # LEFT OUTER JOIN
        "selection",         # WHERE
        "projection",        # SELECT
        "sort",              # ORDER BY
        "limit",             # LIMIT
        "offset",            # OFFSET
        "fullquery",         # Pass the full query to the Platform (even if it does not support all the operators)
        "virtual"            # CREATE TYPE 
    ]

    def __init__(self, *args, **kwargs):
        """
        Constructor
        """
        for key in Capabilities.KEYS:
            object.__setattr__(self, key, False)

        for capability_name in args:
            if capability_name in Capabilities.KEYS:
                object.__setattr__(self, key, True)
            else:
                raise ValueError, "Invalid capability: %s" % capability_name

    def __deepcopy__(self, memo):
        """
        Returns:
            The copy of self.
        """
        capabilities = Capabilities()
        for key in self.KEYS:
            setattr(capabilities, key, getattr(self, key))
        return capabilities

    def __setattr__(self, key, value):
        """
        Enable/Disable a capability from this Capabilities instance.
        Args:
            key: A capability name (see Capabilities.KEYS)
            value: A boolean
        """
        assert key in self.KEYS, "Unknown capability '%s'" % key
        assert isinstance(value, bool)
        object.__setattr__(self, key, value)

    @returns(bool)
    def __getattr__(self, key):
        """
        Retrieve a capability from this Capabilities instance.
        Args:
            key: A capability name (see Capabilities.KEYS)
        Returns:
            The corresponding boolean (True iif enabled).
        """
        assert key in self.KEYS, "Unknown capability '%s'" % key
        object.__getattr__(self, key)

    @returns(bool)
    def is_onjoin(self):
        """
        Test whether a Table is an ONJOIN Table or not. It means that this Table
        cannot be directly queried, but could be involved in a QueryPlan as a
        right operand of a LeftJoin operator.
        Returns:
            True iif the ONJOIN capability is supported.
        """
        return (not self.retrieve) and self.join
 
    @returns(list)
    def to_list(self):
        """
        Returns:
            A list of String where each element is a capability enabled
            in the "self" Capabilities instance.
        """
        return [x for x in self.KEYS if getattr(self, x, False)]

    @returns(dict)
    def to_dict(self):
        """
        Returns:
            The dict corresponding to this Capabilities instance.
        """
        ret = dict()
        for capability in Capabilities.KEYS:
            ret[capability] = True if getattr(self, capability, False) else False
        return ret

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' String related to a Capabilities instance
        """
        list_capabilities = map(lambda x: x if getattr(self, x, False) else '', self.KEYS)
        list_capabilities = ', '.join(self.to_list())
        return '<Capabilities: %s>' % list_capabilities

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' String related to a Capabilities instance
        """
        return self.__str__()

    @returns(bool)
    def __eq__(self, x):
        """
        Compare two Capabilities instances.
        Params:
            x: A Capabilities instance, compared to self.
        Returns:
            True iif self and x provide the same capabilities.
        """
        return set(self.to_list()) == set(x.to_list())

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif the set of Capabilities is empty or not
        """
        return set(self.to_list()) == set()

