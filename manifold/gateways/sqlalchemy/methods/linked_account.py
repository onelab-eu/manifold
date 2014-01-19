#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ModelLinkedAccount wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.models.linked_account import ModelLinkedAccount
from ..methods.sqla_object                              import SQLA_Object

class LinkedAccount(SQLA_Object):
    def __init__(self, gateway):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
        """
        super(LinkedAccount, self).__init__(gateway, ModelLinkedAccount)

