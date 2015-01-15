#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ModelSession wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.sqla_object   import SQLA_Object
from ..models.session                           import ModelSession

class Session(SQLA_Object):
    def __init__(self, gateway, router):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
            router: A manifold Router.
        """
        super(Session, self).__init__(gateway, ModelSession, router)

