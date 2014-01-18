#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# manifold.models.linked_account wrapping
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

from manifold.gateways.sqlalchemy.methods.sqla_object   import SQLA_Object
from manifold.models.linked_account                     import LinkedAccount as ModelLinkedAccount

class LinkedAccount(SQLA_Object):
    def __init__(self, gateway):
        """
        Constructor.
        Args:
            gateway: A SQLAlchemyGateway instance.
        """
        super(LinkedAccount, self).__init__(gateway, ModelLinkedAccount)

