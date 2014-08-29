#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ModelLinkedAccount wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.models.linked_account import ModelLinkedAccount
from ..objects.sqla_object                              import SQLA_Object

class LinkedAccount(SQLA_Object):
    def __init__(self, gateway, router):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
            router: A manifold Router.
        """
        super(LinkedAccount, self).__init__(gateway, ModelLinkedAccount, router)

