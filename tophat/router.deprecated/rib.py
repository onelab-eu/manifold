class RIB(dict):
    """
    Implements a Routing Information Base (RIB).

    A RIB stores a set of announces.
    """
    def __init__(self, dest_cls=object, route_cls=object):
        self.dest_cls = dest_cls
        self.route_cls = route_cls

    #def __setitem__(self, key, value):
    #    if key.__class__ != self.dest_cls:
    #        raise TypeError("Destination of type %s expected in argument. Got %s." % (self.dest_cls.__name__, key.__class__.__name__))
    #    if value.__class__ != self.route_cls:
    #        raise TypeError("Route of type %s expected in argument. Got %s." % (self.route_cls.__name__, value.__class__.__name__))
    #    dict.__setitem__(self, key, value)
