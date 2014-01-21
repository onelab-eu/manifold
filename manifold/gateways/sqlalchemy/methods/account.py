#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ModelAccount wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.models.account    import ModelAccount
from ..methods.sqla_object                          import SQLA_Object

class Account(SQLA_Object):
    def __init__(self, gateway):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
        """
        super(Account, self).__init__(gateway, ModelAccount)

