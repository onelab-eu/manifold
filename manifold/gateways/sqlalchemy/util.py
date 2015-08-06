#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Convenient functions to interact with SQLAlchemy
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC

from manifold.core.filter           import Filter
from manifold.util.type             import accepts, returns

@returns(tuple)
def xgetattr(cls, list_attr):
    """
    Extract attribute values from a Model* instance into a tuple.
    Args:
        cls: Any Model* class.
        list_attr: A list of Strings corresponding to attribute
            names of cls that must be retrieved.
    Returns:
        The corresponding tuple.
    """
    ret = list()
    for a in list_attr:
        ret.append(getattr(cls, a))
    return tuple(ret)

@returns(dict)
def row2dict(row):
    """
    Convert a python object into the corresponding dictionnary, based
    on its attributes.
    Args:
        row: A instance based on a type which is:
            either in manifold.gateways.sqlalchemy.models or
            either in sqlalchemy.util._collections.NamedTuple
    Returns:
        The corresponding dict.
    """
    try:
        from sqlalchemy.util   import NamedTuple
    except ImportError:
        # NamedTuple was renamed in latest sqlalchemy versions
        from sqlalchemy.util   import KeyedTuple as NamedTuple

    # http://stackoverflow.com/questions/18110033/getting-first-row-from-sqlalchemy
    # When you ask specifically for a column of a mapped class with
    # query(Class.attr), SQLAlchemy will return a
    # sqlalchemy.util._collections.NamedTuple instead of DB objects.

    if isinstance(row, NamedTuple):
        return dict(zip(row.keys(), row))
    else:
        return {c.name: getattr(row, c.name) for c in row.__table__.columns}

