#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class implements callback handled while querying a
# User object exposed by a RM. 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA


from manifold.gateways.sfa.object  import Object

class User(Object):
    aliases = {
        "authority"          : "authority_hrn",          # authority it belongs to
        "peer_authority"     : "user_peer_authority",    # ?
        "hrn"                : "user_hrn",               # hrn
        "gid"                : "user_gid",               # gif
        "type"               : "user_type",              # type ???
        "last_updated"       : "user_last_updated",      # last_updated
        "date_created"       : "user_date_created",      # first
        "email"              : "user_email",             # email
        "first_name"         : "user_first_name",        # first_name
        "last_name"          : "user_last_name",         # last_name
        "phone"              : "user_phone",             # phone
        "keys"               : "user_keys",              # OBJ keys !!!
        "reg-slices"         : "slice.slice_hrn",        # OBJ slices
        "reg-pi-authorities" : "pi_authorities",
    }
