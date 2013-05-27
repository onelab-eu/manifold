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

    def __init__(self, type, predicate):
        self.type = type
        self.predicate = predicate

    def get_type(self):
        return self.type

    def get_str_type(self):
        return self.types.get_str(self.type)

    def get_predicate(self):
        return self.predicate

    def __repr__(self):
        return "<Relation %s: %s>" % (self.get_str_type(), self.get_predicate())
