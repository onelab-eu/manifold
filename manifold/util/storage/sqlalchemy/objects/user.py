#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# manifold.models.User wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.objects.sqla_object   import SQLA_Object
from ..models.user                                      import ModelUser

class User(SQLA_Object):
    def __init__(self, gateway, router):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
            router : A manifold Router.
        """
        super(User, self).__init__(gateway, ModelUser, router)
