from manifold.gateways      import Gateway
from manifold.core.platform import Platform
from manifold.core.query    import Query
from manifold.util.storage  import DBStorage as Storage

class Interface(object):
    """
    A manifold standard interface: for a router, a gateway, etc.
    stores metadata, build a query plane from a query
    """
    # Exposes : forward, get_announces, etc.
    # This is in fact a router initialized with a single gateway
    # Better, a router should inherit interface

    def __init__(self, platforms=None, allowed_capabilities=None):
        """
        \brief
        \param platforms A list of platforms and their configuration
        """
        if platforms:
            self.platforms = platforms
        else:
            self.platforms = Storage.execute(Query().get('platform'))

        self.allowed_capabilities = allowed_capabilities
        self.metadata = {}
        self.boot()

    def boot(self):
        if not isinstance(self.platforms, list):
            self.platforms = [self.platforms]
        for platform in self.platforms:
            args = [None, platform.name, None, platform.gateway_config, {}, {}]
            gw = Gateway.get(platform.gateway_name)(*args)
            metadata = gw.get_metadata()
            self.metadata[platform.name] = {m.table.class_name:m for m in metadata}

    def instanciate_gateways(self, query_plan):
        for from_node in query_plan.froms:
            assert len(from_node.table.platforms) == 1, "Several platforms declared in FROM node"
            name = iter(from_node.table.platforms).next()

            platform = [p for p in self.platforms if p.name == name]
            if not platform:
                raise Exception, "Cannot find platform data for '%s'" % name
            platform = platform[0]

            # XXX This should be simplified
            args = [None, name, None, platform.gateway_config, {}, {}]
            gw = Gateway.get(platform.gateway_name)(*args)

            gw.set_query(from_node.query)
            from_node.set_gateway(gw)


    def forward(self, query):
        raise Exception, 'Not implemented in base class'
