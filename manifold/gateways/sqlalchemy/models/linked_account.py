#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A LinkedAccount is used to link the Manifold Account
# to an Account related to a given Platform.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC


import json
from sqlalchemy                 import Column, ForeignKey, Integer, String
from sqlalchemy.orm             import relationship

from ..models                   import Base

class ModelLinkedAccount(Base):
    __tablename__ = "linked_account"

    platform_id = Column(Integer, ForeignKey("platform.platform_id"), primary_key = True, doc = "Platform identifier")
    user_id     = Column(Integer, ForeignKey("user.user_id"),         primary_key = True, doc = "User identifier")
    identifier  = Column(String,                                                          doc = "Identifier")

    user        = relationship("ModelUser",     uselist = False)
    platform    = relationship("ModelPlatform", uselist = False)
