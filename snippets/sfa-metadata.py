#!/usr/bin/env python

import sqlalchemy
from sqlalchemy.orm import class_mapper

from sfa.storage.model import RegSlice

from manifold.core.announce import Announce
from manifold.core.field    import Field
from manifold.core.table    import Table
from manifold.util.type     import accepts, returns
from manifold.gateways.sqlalchemy.objects.sqla_object                          import SQLA_Object

#-------------------------------------------------------------------------------
# sql alchemy helpers
#-------------------------------------------------------------------------------

# Common code with ../manifold/gateways/sqlalchemy/objects/sqla_object.py

# http://stackoverflow.com/questions/2537471/method-of-iterating-over-sqlalchemy-models-defined-columns
def attribute_names(cls):
    return [prop.key for prop in class_mapper(cls).iterate_properties
            if isinstance(prop, sqlalchemy.orm.ColumnProperty)]

# Primary key should have a basetype and not a relation (here due to # inheritance)
# We still miss foreign keys

@returns(Announce)
def make_announce(model):
    """
    Returns:
        The list of Announce instances related to this object.
    """
    # Only when we have an instance
    #model = self.get_model()

    #table_name = model.__class__.__name__.lower()
    table_name = model.__tablename__

    table = Table('sfa', table_name)

    primary_key = tuple()

    # If we look only in model.__table__.columns we lose fields inherited
    # http://docs.sqlalchemy.org/en/rel_0_9/orm/inheritance.html
    for prop in class_mapper(model).iterate_properties:
        # prop is a RelationshipProperty
        if not isinstance(prop, sqlalchemy.orm.ColumnProperty):
            continue
        for column in prop.columns:
            fk = column.foreign_keys
            if fk:
                fk = iter(column.foreign_keys).next()
                _type = fk.column.table.name
            else:
                _type = SQLA_Object._map_types[column.type.__class__]

            # We hardcode that certain fields are local
            _qualifiers = list() # ['const', 'local']
            if column.name in ['auth_type', 'config']:
                _qualifiers.append('local')

            # Multiple foreign keys are not handled yet
            field = Field(
                name        = column.name,
                type        = _type,
                qualifiers  = _qualifiers, 
                is_array    = False,
                description = column.description
            )
            print "field:", field
            table.insert_field(field)

            if column.primary_key:
                primary_key += (column.name, )
        
    print "pk:", primary_key
    table.insert_key(primary_key)

    table.capabilities.retrieve   = True 
    table.capabilities.join       = True
    table.capabilities.selection  = True
    table.capabilities.projection = True

    return Announce(table)



print make_announce(RegSlice)
