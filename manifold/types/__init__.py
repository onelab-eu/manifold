from manifold.types.int      import int
from manifold.types.string   import string
from manifold.types.date     import date
from manifold.types.inet     import inet
from manifold.types.hostname import hostname

map_name_type = {
    'string'  : string,
    'int'     : int,
    'date'    : date,
    'inet'    : inet,
    'hostname': hostname
}

BASE_TYPES = ['int', 'string', 'date'] #, 'inet', 'hostname']

def type_by_name(name):
    return map_name_type[name]

def type_get_name(type):
    return type.__typename__
