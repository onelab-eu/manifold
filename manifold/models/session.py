#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Session class.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC

import time, crypt, base64, random
from hashlib              import md5

from sqlalchemy           import Column, ForeignKey, Integer, String
from sqlalchemy.orm       import relationship, backref

from manifold.models      import Base 
from manifold.models.user import User 

class Session(Base):

    restrict_to_self = True

    session = Column(String, primary_key=True, doc="Session identifier")
    expires = Column(Integer, doc="Session identifier")
    user_id = Column(Integer, ForeignKey('user.user_id'), doc='User of the session')
    user = relationship("User", backref="sessions", uselist = False)

    @classmethod
    def process_params(cls, params, filters, user):
        # Generate session ID
        if not 'session' in params:
            bytes = random.sample(xrange(0, 256), 32)
            # Base64 encode their string representation
            params['session'] = base64.b64encode("".join(map(chr, bytes)))

        # Set expiration date
        if not 'expires' in params:
            params['expires'] = int(time.time()) + (24 * 60 * 60)

        User.params_ensure_user(params, user)
