#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class implements callback handled while querying a
# Authority object exposed by a RM. 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from rm_object import RM_Object

class Authority(RM_Object):
    aliases = {
        'hrn'               : 'authority_hrn',                  # hrn
        'reg-pis'           : 'pi_users',
#        'persons'           : 'user',
    }
