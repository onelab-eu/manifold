#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class implements callback handled while querying a
# User object exposed by a RM. 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from rm_object  import RM_Object

class User(RM_Object):
    aliases = {
        # REGISTRY FIELDS
        'hrn'               : 'user_hrn',
        'type'              : 'user_type',
        'email'             : 'user_email',
        'gid'               : 'user_gid',
        'authority'         : 'parent_authority',
        'reg-keys'          : 'keys',
        'reg-slices'        : 'slices',
        'reg-pi-authorities': 'pi_authorities',

        # TESTBED FIELDS
        'first_name'        : 'user_first_name',
        'last_name'         : 'user_last_name',
        'phone'             : 'user_phone',
        'enabled'           : 'user_enabled',
        'keys'              : 'keys',

        # UNKNOWN
        'peer_authority'    : 'user_peer_authority',
        'last_updated'      : 'user_last_updated',
        'date_created'      : 'user_date_created',

    }
