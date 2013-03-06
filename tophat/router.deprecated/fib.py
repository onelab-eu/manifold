class FIB(dict):
    """
    Implements a Forwarding Information Base (FIB).
    """
    def __init__(self, dest_cls=object, route_cls=object):
        self.dest_cls = dest_cls
        self.route_cls = route_cls

    def __setitem__(self, key, value):
        if type(key) != self.dest_cls:
            raise TypeError("Destination of type %s expected in argument. Got %s." % (type(element), self.announce_cls))
        if type(value) != self.route_cls:
            raise TypeError("Route of type %s expected in argument. Got %s." % (type(element), self.announce_cls))
        super(FIB, self).__setitem__(self, key, value)

    def get_most_specific_route(self):
        return None
