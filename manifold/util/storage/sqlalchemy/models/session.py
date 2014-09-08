#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Session class.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC

import time, base64, random

from sqlalchemy                                 import Column, ForeignKey, Integer, String
from sqlalchemy.orm                             import relationship, backref

from manifold.gateways.sqlalchemy.models.model  import Model
from manifold.util.type                         import accepts, returns 
from ..models.user                              import ModelUser 

class ModelSession(Model):

    restrict_to_self = True

    session = Column(String, primary_key = True,          doc = "Session identifier")
    expires = Column(Integer,                             doc = "Expiration date of this Session")
    user_id = Column(Integer, ForeignKey("user.user_id"), doc = "User of the Session")

    user    = relationship("ModelUser", backref = "sessions", uselist = False)

    @staticmethod
    @returns(dict)
    def process_params(params, filters, user, router, db_session):
        # Generate session ID
        if not "session" in params:
            bytes = random.sample(xrange(0, 256), 32)
            # Base64 encode their string representation
            params["session"] = base64.b64encode("".join(map(chr, bytes)))

        # Set expiration date
        if not "expires" in params:
            params["expires"] = int(time.time()) + (24 * 60 * 60)

        ModelUser.params_ensure_user(params, user, db_session)
