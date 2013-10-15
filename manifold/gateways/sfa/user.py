#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manifold user management.
# TODO this might be merged with manifold/models/user.py, but
# for now, this is specific to SFA.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC

from types                              import StringTypes
from manifold.util.log                  import Log 
from manifold.util.type                 import accepts, returns 

ADMIN_USER = {
    "email" : "admin"
}

def check_user(user):
    """
    Tests whether a user is well-formed
    Args:
        user: A dictionnary describing an User. 
    """
    assert isinstance(user, dict), "Invalid user: %s (%s)" % (user, type(user))

@returns(bool)
def is_user_admin(user):
    """
    Tests whether a User is admin or not.
    Args:
        user: A dictionnary (having at least "email" key) corresponding to the User.
    Returns:
        True iif this User is admin.
    """
    check_user(user)
    return user["email"] == ADMIN_USER["email"]

@returns(int)
def get_user_hash(user):
    """
    (Internal usage)
    Compute the hash corresponding to a User.
    Args:
        user: A dictionnary describing the User issuing the SFA Query.
    """
    check_user(user)
    return hash(frozenset(user.items()))

