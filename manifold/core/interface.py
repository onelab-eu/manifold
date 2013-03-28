from manifold.gateways      import Gateway
from manifold.core.platform import Platform
from manifold.core.query    import Query
from manifold.util.storage  import DBStorage as Storage
from manifold.models        import *
import json

class Interface(object):
    """
    A manifold standard interface: for a router, a gateway, etc.
    stores metadata, build a query plane from a query
    """
    # Exposes : forward, get_announces, etc.
    # This is in fact a router initialized with a single gateway
    # Better, a router should inherit interface

    LOCAL_NAMESPACE = "local"

    def __init__(self, platforms=None, allowed_capabilities=None):
        """
        \brief
        \param platforms A list of platforms and their configuration
        """
        print "PLATFORMS=", platforms
        if platforms:
            self.platforms = platforms
        else:
            self.platforms = Storage.execute(Query().get('platform').filter_by('disabled', '=', False), format='object')
        self.allowed_capabilities = allowed_capabilities
        self.metadata = {}
        self.boot()

    def boot(self):
        if not isinstance(self.platforms, list):
            self.platforms = [self.platforms]
        for platform in self.platforms:
            args = [None, platform.name, None, platform.gateway_config, {}, None]
            gw = Gateway.get(platform.gateway_name)(*args)
            metadata = gw.get_metadata()
            self.metadata[platform.name] = {m.table.class_name:m for m in metadata}

    def instanciate_gateways(self, query_plan, user):
        """
        \brief instanciate gateway instances in the query plane
        \param query_plan (QueryPlan)
        \param user (dict)
        \sa manifold.core.query_plan
        """
        try:
            for from_node in query_plan.froms:
                assert len(from_node.table.platforms) == 1, "Several platforms declared in FROM node"
                name = iter(from_node.table.platforms).next()

                platform = [p for p in self.platforms if p.name == name]
                if not platform:
                    raise Exception, "Cannot find platform data for '%s'" % name
                platform = platform[0]

                # User account information
                accounts = [a for a in user.accounts if a.platform.platform == platform.name]
                if not accounts:
                    raise Exception, 'No such account'
                account = accounts[0]

                if not account.auth_type or account.auth_type == 'default':
                    print "W: should set account auth type"
                    continue

                if account.auth_type == 'user':
                    args = [None, name, None, platform.gateway_config, json.loads(account.config), user]
                elif account.auth_type == 'reference':
                    ref_platform = json.loads(account.config)['reference_platform']
                    # XXX STORAGE
                    ref_platform = db.query(Platform).filter(Platform.platform == ref_platform).one()
                    ref_accounts = [a for a in user.accounts if a.platform == ref_platform]
                    if not ref_accounts:
                        raise Exception, "reference account does not exist"
                    ref_account = ref_accounts[0]

                    args = [None, name, None, platform.gateway_config, json.loads(ref_account.config), user]
                    
                else:
                    raise Exception('auth type not implemented: %s' % account.auth_type)

                gw = Gateway.get(platform.gateway_name)(*args)

                gw.set_query(from_node.query)
                from_node.set_gateway(gw)
        except Exception, e:
            print "EXC inst gw", e


    def forward(self, query):
        raise Exception, 'Not implemented in base class'
