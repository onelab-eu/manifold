# -*- coding: utf-8 -*-
#
# A Platform represents a source of data. A Platform is related
# to a Gateway, which wrap this source of data in the Manifold
# framework. For instance, TDMI is a Platform using the PostgreSQL
# Gateway.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import json
from sqlalchemy                                 import Column, Integer, String, Boolean, Enum

from manifold.gateways.sqlalchemy.models.model  import Model

class ModelPlatform(Model):
    platform_id          = Column(Integer, doc = "Platform identifier", primary_key = True)
    platform             = Column(String,  doc = "Platform name", unique = True)
    platform_longname    = Column(String,  doc = "Platform long name")
    platform_description = Column(String,  doc = "Platform description")
    platform_url         = Column(String,  doc = "Platform URL")
    deleted              = Column(Boolean, doc = "Platform has been deleted", default = False)
    disabled             = Column(Boolean, doc = "Platform has been disabled", default = False)
    status               = Column(String,  doc = "Platform status")
    status_updated       = Column(Integer, doc = "Platform last check")
    platform_has_agents  = Column(Boolean, doc = "Platform has agents", default = False)
    first                = Column(Integer, doc = "First timestamp, in seconds since UNIX epoch")
    last                 = Column(Integer, doc = "Last timestamp, in seconds since UNIX epoch")
    gateway_type         = Column(String,  doc = "Type of the gateway to use to connect to this platform")
    auth_type            = Column(Enum("none", "default", "user", "reference", "managed"), default = "default")
    config               = Column(String,  doc = "Default configuration (serialized in JSON)")

