#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class implements callback handled while querying a
# Authority object exposed by a RM. 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from manifold.gateways.sfa.rm.methods       import Object

class Authority(Object):
    aliases = {
        "hrn"       : "authority_hrn",
        "PI"        : "pi_users",
    }
