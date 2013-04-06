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
        self.tables = []
        for platform in self.platforms:
            args = [None, platform.name, None, platform.gateway_config, {}, None]
            gw = Gateway.get(platform.gateway_name)(*args)
            metadata = gw.get_metadata()
            #self.metadata[platform.name] = {m.table.name:m for m in metadata}
            #self.metadata[platform.name] = {m.table.class_name:m for m in metadata}
            for m in metadata:
                self.tables.append(m.table)


    def get_gateway_config(self, gateway_name):
        log_info("Hardcoded CSV|PostgreSQL configuration")
        if gateway_name == 'postgresql':
            config = {'db_user':'postgres', 'db_password':None, 'db_name':'test'}
        elif gateway_name == 'csv':
            config = {'filename': '/tmp/test.csv'}
        else:
            config = {}
        return config

    def instanciate_gateways(self, query_plan, user):
        """
        \brief instanciate gateway instances in the query plane
        \param query_plan (QueryPlan)
        \param user (dict)
        \sa manifold.core.query_plan
        """
        # XXX Platforms only serve for metadata
        # in fact we should initialize filters from the instance, then rely on
        # Storage including those filters...
        try:
            for from_node in query_plan.froms:
                name = from_node.get_platform()

                platform = [p for p in self.platforms if p.name == name]
                if not platform:
                    raise Exception, "Cannot find platform data for '%s'" % name
                platform = platform[0]

                if name == 'dummy':
                    args = [None, name, None, platform.gateway_config, None, user]
                else:

                    if not platform.auth_type:
                        print "W: should set account auth type"
                        continue

                    # XXX platforms might have multiple auth types (like pam)
                    # XXX we should refer to storage

                    if platform.auth_type == 'none':
                        config = {}

                    # For default, take myslice account

                    elif platform.auth_type == 'user':
                        # User account information
                        accounts = [a for a in user.accounts if a.platform.platform == platform.name]
                        if not accounts:
                            raise Exception, 'No such account'
                        account = accounts[0]

                        config = json.loads(account.config)

                        if account.auth_type == 'reference':
                            ref_platform = config['reference_platform']
                            ref_platform = db.query(Platform).filter(Platform.platform == ref_platform).one()
                            ref_accounts = [a for a in user.accounts if a.platform == ref_platform]
                            if not ref_accounts:
                                raise Exception, "reference account does not exist"
                            ref_account = ref_accounts[0]

                            config = json.loads(ref_account.config)

                    else:
                        raise Exception('auth type not implemented: %s' % platform.auth_type)
                
                args = [None, name, None, platform.gateway_config, config, user]
                gw = Gateway.get(platform.gateway_name)(*args)

                gw.set_query(from_node.query)
                from_node.set_gateway(gw)
        except Exception, e:
            print "EXC inst gw", e


    def forward(self, query):
        raise Exception, 'Not implemented in base class'
