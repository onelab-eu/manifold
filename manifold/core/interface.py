from manifold.gateways      import Gateway
from manifold.core.platform import Platform

class Interface(object):
    """
    A manifold standard interface: for a router, a gateway, etc.
    stores metadata, build a query plane from a query
    """
    # Exposes : forward, get_announces, etc.
    # This is in fact a router initialized with a single gateway
    # Better, a router should inherit interface

    def __init__(self, platforms):
        """
        \brief
        \param platforms A list of platforms and their configuration
        """
        self.platforms = platforms
        self.metadata = {}
        self.boot()

    def boot(self):
        for platform in self.platforms:
            args = [None, platform.name, None, platform.gateway_config, {}, {}]
            gw = Gateway.get(platform.gateway_name)(*args)
            # A set of announces
            self.metadata[platform.name] = gw.get_metadata()

    def forward(self, query):
        raise Exception, 'Not implemented in base class'
