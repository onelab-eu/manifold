class FIB(set):
    """
    Implements a Forwarding Information Base (FIB).
    """
    def __init__(self, announce_cls=object):
        self.announce_cls = announce_cls

    def add(self, element):
        if type(element) != self.announce_cls:
            raise TypeError("Announce of type %s expected in argument. Got %s." % (type(element), self.announce_cls))
        super(FIB, self).add(element)

    def get_most_specific_route(self):
        return None
