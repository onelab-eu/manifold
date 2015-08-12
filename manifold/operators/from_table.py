#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# FromTable:
#    A FromTable Node stores a list of homogeneous static
#    records. It is used in the AST to feed the Manifold
#    while the QueryPlan execution.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                          import StringTypes
from manifold.core.capabilities     import Capabilities
from manifold.core.address          import Address
from manifold.core.node             import Node
from manifold.core.record           import Record, Records
from manifold.util.type             import returns, accepts

DUMPSTR_FROMTABLE  = "SELECT %s FROM [%r, ...]"

class FromTable(Node):
    """
    A FromTable Node stores a list of homogeneous records (e.g.
    a list of records that we could store in a same given Table
    having a given key). It behaves like a standard From Node.
    """

    def __init__(self, destination, records, key):
        """
        Constructor
        Args:
            destination: This Address corresponds to the one we would use the
                list of embeded records from a Table.
            records: A list of homogeneous Record instances
                (same fields, same key).
            key: The Key instance related to these Records.
        """
        assert isinstance(destination,   Address), "Invalid destination = %r (%r)"   % (destination,   type(destination))
        assert isinstance(records, list),  "Invalid records = %r (%r)" % (records, type(records))

        super(FromTable, self).__init__(max_producers = 0)
        self.destination, self.records, self.key = destination, Records(records), key

    @returns(StringTypes)
    def __repr__(self, indent = 0):
        """
        Returns:
            The "%s" representation of this FromTable Node.
        """
        if self.records:
            fields = self.get_destination().get_field_names()
            return DUMPSTR_FROMTABLE % (
                "*" if fields.is_star() else ", ".join([field for field in fields]),
                self.records[0]
            )
        else:
            return "EMPTY"
