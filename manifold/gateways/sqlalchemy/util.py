#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Convenient functions to interact with SQLAlchemy 
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

from __future__                     import absolute_import

from manifold.core.filter           import Filter
from manifold.core.record           import Record
from manifold.util.predicate        import included
from manifold.util.type             import accepts, returns

@returns(tuple)
def xgetattr(cls, list_attr):
    """
    Extract attributes from an instance to make the corresponding tuple.
    Args:
        cls:
        list_attr: A list of Strings corresponding to attribute
            names of cls that must be retrieved.
    Returns:
        The corresponding tuple.
    """
    ret = list()
    for a in list_attr:
        ret.append(getattr(cls, a))
    return tuple(ret)

def make_clause(cls, key, op, value):
    """
    (Internal usage)
    Args:
        cls:
        key:
        op:
        value:
    Returns:
    """
    key_attr = getattr(cls, key)
    return key_attr.in_(value) if op == included else op(key_attr, value)
    
def make_sqla_filter(cls, predicate):
    """
    (Internal usage)
    Args:
        cls:
        predicate: A Predicate instance.
    Returns:
        The corresponding SQLAlchemy filter.
    """
    key, op, value = predicate.get_tuple()

    if isinstance(key, tuple):
        # Recursive building of the clause ( AND ) for tuples
        return reduce(lambda x,y: x and make_clause(cls, y[0], op, y[1]), zip(key, value), True)
    else:
        return make_clause(cls, key, op, value)

@returns(list)
def make_sqla_filters(cls, predicates):
    """
    Convert a Filter into a list of sqlalchemy filters.
    Args:
        cls:
        predicates: A Filter instance or None.
    Returns:
        The corresponding filters (or None if predicates == None).
    """
    if predicates:
        _filters = list() 
        for predicate in predicates:
            f = make_sqla_filter(cls, predicate)
            _filters.append(f)
        return _filters
    else:
        return None

@returns(Record)
def row2record(row):
    """
    Convert a python object into the corresponding dictionnary, based
    on its attributes.
    Args:
        row: A instance based on a type which is
            either in manifold/models
            or either sqlalchemy.util._collections.NamedTuple
    Returns:
        The corresponding Record.
    """
    try:
        from sqlalchemy.util._collections   import NamedTuple
    except ImportError:
        # NamedTuple was renamed in latest sqlalchemy versions
        from sqlalchemy.util._collections   import KeyedTuple as NamedTuple

    # http://stackoverflow.com/questions/18110033/getting-first-row-from-sqlalchemy
    # When you ask specifically for a column of a mapped class with
    # query(Class.attr), SQLAlchemy will return a
    # sqlalchemy.util._collections.NamedTuple instead of DB objects.

    if isinstance(row, NamedTuple):
        return Record(zip(row.keys(), row))
    else:
        return Record({c.name: getattr(row, c.name) for c in row.__table__.columns})

