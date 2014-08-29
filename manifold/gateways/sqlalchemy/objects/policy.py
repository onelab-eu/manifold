#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ModelsPolicy wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.models.policy import ModelPolicy
from ..objects.sqla_object                      import SQLA_Object

class Policy(SQLA_Object):
    def __init__(self, gateway, router):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
            router: A manifold Router.
        """
        super(Policy, self).__init__(gateway, ModelPolicy, router)

