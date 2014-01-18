#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# manifold.models.Session wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.methods.sqla_object   import SQLA_Object
from manifold.models.session                            import Session as ModelSession

class Session(SQLA_Object):
    def __init__(self, gateway):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
        """
        super(Session, self).__init__(gateway, ModelSession)

