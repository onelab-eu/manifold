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

