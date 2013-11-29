#use Consumer class instead

#DEPRECATED|#!/usr/bin/env python
#DEPRECATED|# -*- coding: utf-8 -*-
#DEPRECATED|#
#DEPRECATED|# A Receiver is passed to a forward method (see Router, Gateway...) 
#DEPRECATED|# to store the ResultValue corresponding to a Query.
#DEPRECATED|#
#DEPRECATED|# A From Node, a Shell should inherits Receiver since they forward
#DEPRECATED|# Query instances to a Router.
#DEPRECATED|#
#DEPRECATED|# Copyright (C) UPMC Paris Universitas
#DEPRECATED|# Authors:
#DEPRECATED|#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#DEPRECATED|
#DEPRECATED|from manifold.core.record           import Record
#DEPRECATED|from manifold.core.result_value     import ResultValue
#DEPRECATED|from manifold.util.type             import accepts, returns
#DEPRECATED|
#DEPRECATED|class Receiver(object):
#DEPRECATED|    def __init__(self):
#DEPRECATED|        """
#DEPRECATED|        Constructor.
#DEPRECATED|        """
#DEPRECATED|        self.result_value = None # ResultValue corresponding to the last Query
#DEPRECATED|
#DEPRECATED|    @returns(ResultValue)
#DEPRECATED|    def get_result_value(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The ResultValue corresponding to the last issued Query.
#DEPRECATED|        """
#DEPRECATED|        return self.result_value
#DEPRECATED|
#DEPRECATED|    def set_result_value(self, result_value):
#DEPRECATED|        """
#DEPRECATED|        Function called back by self.interface.forward() once the Query
#DEPRECATED|        has been executed.
#DEPRECATED|        Args:
#DEPRECATED|            result_value: A ResultValue or None.
#DEPRECATED|        """
#DEPRECATED|        assert not result_value or isinstance(result_value, ResultValue), \
#DEPRECATED|            "Invalid result_value = %s (%s)" % (result_value, type(result_value))
#DEPRECATED|        self.result_value = result_value
#DEPRECATED|
#DEPRECATED|###############
#DEPRECATED|# CODE jetable
#DEPRECATED|# <<
#DEPRECATED|###############
#DEPRECATED|
#DEPRECATED|    @returns(int)
#DEPRECATED|    def get_identifier(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            An integer identifying the Receiver. This identifier is only used
#DEPRECATED|            for debug purposes.
#DEPRECATED|        """
#DEPRECATED|        return 0
#DEPRECATED|
#DEPRECATED|    def default_callback(self, record):
#DEPRECATED|        """
#DEPRECATED|        Accumulate incoming Record in the nested ResultValue.
#DEPRECATED|        Args:
#DEPRECATED|            record: A Record instance.
#DEPRECATED|        """
#DEPRECATED|        try:
#DEPRECATED|            x = self.records
#DEPRECATED|        except AttributeError:
#DEPRECATED|            self.records = list()
#DEPRECATED|
#DEPRECATED|        if record.is_last():
#DEPRECATED|            result_value = ResultValue.get_success(self.records)
#DEPRECATED|            del self.records
#DEPRECATED|            self.set_result_value(result_value)
#DEPRECATED|        else:
#DEPRECATED|            self.records.append(record)
#DEPRECATED|
#DEPRECATED|    def callback(self, record):
#DEPRECATED|        """
#DEPRECATED|        This method is called back whenever the Receiver receives a Record.
#DEPRECATED|        A class inheriting Receiver may overwrite this method.
#DEPRECATED|        Args:
#DEPRECATED|            record: A Record instance.
#DEPRECATED|        """
#DEPRECATED|        return self.default_callback(record)
#DEPRECATED|
#DEPRECATED|###############
#DEPRECATED|# CODE jetable
#DEPRECATED|# >>
#DEPRECATED|###############
