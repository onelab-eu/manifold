from manifold.core.capabilities import Capabilities

class Announce(object):
    def __init__(self, table, capabilities, cost=None):
        """
        \brief Constructor
        """
        assert isinstance(capabilities, Capabilities), "Wrong capabilities argument"

        self.table = table
        self.capabilities = capabilities
        self.cost = cost
