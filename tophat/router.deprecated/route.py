class Route:
    """
    Implements a route.

    TODO:

    < tells a route is most specific, if it concerns a destination included in the other route
    """
    def __init__(self, destination, peer, cost, timestamp):
        self.destination = destination
        self.peer = peer
        self.cost = cost
        self.timestamp = timestamp
