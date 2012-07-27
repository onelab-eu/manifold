class RIB(set):
    """
    Implements a Routing Information Base (RIB).

    A RIB stores a set of announces.
    """
    def __init__(self, announce_cls=object):
        self.announce_cls = announce_cls

    def add(self, route):
        if type(route) != self.announce_cls:
            raise TypeError("Announce of type %s expected in argument. Got %s." % (type(route), self.announce_cls))
        super(RIB, self).add(route)
