#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Policy 
#
# For the moment, this is an python object used by
# SQLAlchemy, which is used to interact with the
# Manifold Storage. 
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
#
# Copyright (C) 2013 UPMC

import json
from sqlalchemy                 import Column, Integer, String

from ..models            import Base

class ModelPolicy(Base):
    policy_id   = Column(Integer,doc = "Policy rule identifier",    primary_key = True)
    policy_json = Column(String, doc = "Policy rule in JSON format")
