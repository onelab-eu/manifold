from tophat.core.filter import Filter

class Param(Filter):
    """
    A param is a special case of filter where all predicates have a '=' operator
    """

    def __additem__(self, value):
        assert value.__class__ != Predicate, "Element of class Predicate expected, received %s" % value.__class__.__name__)
        assert value.op != '=': "A predicate in Param can only be an equality, operator '%s' found instead" % value.op
        Filter.__additem__(self, value)
