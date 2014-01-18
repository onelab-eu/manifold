#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# manifold.models.Policy wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.methods.sqla_object   import SQLA_Object
from manifold.models.policy                             import Policy as ModelPolicy

class Policy(SQLA_Object):
    def __init__(self, gateway):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
        """
        super(Policy, self).__init__(gateway, ModelPolicy)

