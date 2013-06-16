from manifold.util.misc import Enum

# A relation between two tables of the dbgraph
class Relation(object):

    types = Enum(
        'SPECIALIZATION',
        'PARENT',
        'CHILD',
#        'COLLECTION',
        'LINK_11', 
        'LINK_1N',
        'LINK_1N_BACKWARDS',
        'LINK'
    )

    def __init__(self, type, predicate, name=None):
        self.type = type
        self.predicate = predicate
        self.name = name

    def get_type(self):
        return self.type

    def get_str_type(self):
        return self.types.get_str(self.type)

    def get_predicate(self):
        return self.predicate

    def __repr__(self):
        return "<Relation<%s> %s: %s>" % (self.name if self.name else '', self.get_str_type(), self.get_predicate())

    def get_relation_name(self):
        return self.name
        
    def requires_subquery(self):
        return self.type not in [Relation.types.LINK, Relation.types.CHILD, Relation.types.PARENT]

    def requires_join(self):
        return not self.requires_subquery()
