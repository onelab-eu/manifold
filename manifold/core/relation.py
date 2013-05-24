from manifold.util.misc import Enum

# A relation between two tables of the dbgraph
class Relation(object):

    types = Enum(
        'SPECIALIZATION',
        'INHERITANCE',
        'LINK_11', 
        'LINK_1N',
        'LINK'
    )

    def __init__(self, type, fields_u, predicate):
        self.type = type
        self.fields_u = fields_u
        self.predicate = predicate

    def get_type(self):
        return self.type

    def get_str_type(self):
        return self.types.get_str(self.type)

    def get_fields_u(self):
        return self.fields_u

    def get_predicate(self):
        return self.predicate

    def __repr__(self):
        return "<Relation %s: %s>" % (self.get_str_type(), self.get_predicate())
