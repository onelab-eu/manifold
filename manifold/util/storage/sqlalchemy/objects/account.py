#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ModelAccount wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.objects.sqla_object   import SQLA_Object
from ..models.account                                   import ModelAccount

class Account(SQLA_Object):
    def __init__(self, gateway, router):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
            router: A Manifold router.
        """
        super(Account, self).__init__(gateway, ModelAccount, router)

