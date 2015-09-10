#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Record and Records classes.
#
# A Record is a Packet transporting value resulting of a Query.
# A Record behaves like a python dictionnary where:
# - each key corresponds to a field name
# - each value corresponds to the corresponding field value.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Loïc Baron        <loic.baron@lip6.fr>

from manifold.core.packet       import Packet
from manifold.util.type         import accepts, returns

#-------------------------------------------------------------------------------
# Records class
#-------------------------------------------------------------------------------

class Record(Packet):
    def __init__(self, *args, **kwargs):
        last = kwargs.pop('last', False)
        receiver = kwargs.pop('receiver', None)
        super(Record, self).__init__(
            receiver = receiver,
            last     = last,
            protocol = Packet.PROTOCOL_QUERY
        )

        assert len(args) in [0,1]
        if len(args) > 0:
            self.update_data(args[0])
        if kwargs:
            self.update_data(kwargs)

    @staticmethod
    def from_dict(dic):
        record = Record()
        record.set_dict(dic)
        return record


class Records(list):
    """
    A Records instance transport a list of Record instances.
    """

    def __init__(self, itr = None):
        """
        Constructor.
        Args:
            itr: An Iterable instance containing instances that
                can be casted into a Record (namely dict or
                Record instance). For example, itr may be
                a list of dict (having the same keys).
        """
        if itr:
            list.__init__(self, [(x if isinstance(x, Record) else Record(x)) for x in itr])
        else:
            list.__init__(self)

    @returns(list)
    def to_dict_list(self):
        """
        Returns:
            The list of Record instance corresponding to this
            Records instance.
        """
        return [record.to_dict() for record in self]

    to_list = to_dict_list

    def get_one(self):
        return self[0]

    def get_field_names(self):
        return self.get_one().get_field_names()

    def add_record(self, record):
        self.append(record)

    def add_records(self, records):
        self.extend(records)
