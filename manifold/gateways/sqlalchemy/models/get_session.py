#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# get_session is used to abstract the session retrieval
# which depends on the version of sqlalchemy.
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC

def get_session(obj):
    """
    Retrieve the session use to connect to the database
    through sqlalchemy.
    """
    # The way the session is retrieved has changed
    # http://docs.sqlalchemy.org/en/latest/core/inspection.html
    try:
        # sqlalchemy >= 0.9
        from sqlalchemy import inspect
        return inspect(obj).session
    except ImportError:
        # sqlalchemy < 0.9
        # http://docs.sqlalchemy.org/en/latest/orm/session.html
        # http://stackoverflow.com/questions/3885601/sqlalchemy-get-object-instance-state
        from sqlalchemy.orm import object_session 
        return object_session(obj)

