#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Receiver is passed to a forward method (see Router, Gateway...) 
# to store the ResultValue corresponding to a Query.
#
# A From Node, a Shell should inherits Receiver since they forward
# Query instances to a Router.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from manifold.core.record           import Record
from manifold.core.result_value     import ResultValue
from manifold.util.type             import accepts, returns

class Receiver(object):
    def __init__(self):
        """
        Constructor.
        """
        self.result_value = None # ResultValue corresponding to the last Query

    @returns(ResultValue)
    def get_result_value(self):
        """
        Returns:
            The ResultValue corresponding to the last issued Query.
        """
        return self.result_value

    def set_result_value(self, result_value):
        """
        Function called back by self.interface.forward() once the Query
        has been executed.
        Args:
            result_value: A ResultValue or None.
        """
        assert not result_value or isinstance(result_value, ResultValue), \
            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
        self.result_value = result_value

###############
# CODE jetable
# <<
###############

    @returns(int)
    def get_identifier(self):
        """
        Returns:
            An integer identifying the Receiver. This identifier is only used
            for debug purposes.
        """
        return 0

    def default_callback(self, record):
        """
        Accumulate incoming Record in the nested ResultValue.
        Args:
            record: A Record instance.
        """
        try:
            x = self.records
        except AttributeError:
            self.records = list()

        if record.is_last():
            result_value = ResultValue.get_success(self.records)
            del self.records
            self.set_result_value(result_value)
        else:
            self.records.append(record)

    def callback(self, record):
        """
        This method is called back whenever the Receiver receives a Record.
        A class inheriting Receiver may overwrite this method.
        Args:
            record: A Record instance.
        """
        return self.default_callback(record)

###############
# CODE jetable
# >>
###############
