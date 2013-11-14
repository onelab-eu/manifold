#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions used to manage passwords
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

import crypt
from types                          import StringTypes

@returns(StringTypes)
def hash_password(password):
    """
    Args:
        password: A String instance containing a password to hash.
    Returns:
        The corresponding hash (String instance).
    """
    magic = "$1$"
    password = password
    # Generate a somewhat unique 8 character salt string
    salt = str(time.time()) + str(Random().random())
    salt = md5(salt).hexdigest()[:8]

    if len(password) <= len(magic) or password[0:len(magic)] != magic:
        password = crypt.crypt(password.encode('latin1'), magic + salt + "$")

    return password 


