from manifold.gateways          import Gateway
from manifold.core.platform     import Platform
from manifold.core.query        import Query
from manifold.util.storage      import DBStorage as Storage
from manifold.models            import *
from manifold.core.result_value import ResultValue
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
        #self.tables = []
        for platform in self.platforms:
            args = [None, platform.name, None, platform.gateway_config, {}, None]
            gw = Gateway.get(platform.gateway_name)(*args)
            metadata = gw.get_metadata()
            #self.metadata[platform.name] = {m.table.name:m for m in metadata}
            #self.metadata[platform.name] = {m.table.class_name:m for m in metadata}
            self.metadata[platform] = []
            for m in metadata:
                self.metadata[platform].append(m)
                #self.tables.append(m.table)

    def get_gateway_config(self, gateway_name):
        log_info("Hardcoded CSV|PostgreSQL configuration")
        if gateway_name == 'postgresql':
            config = {'db_user':'postgres', 'db_password':None, 'db_name':'test'}
        elif gateway_name == 'csv':
            config = {'filename': '/tmp/test.csv'}
        else:
            config = {}
        return config

    def instanciate_gateways(self, query_plan, user, timestamp = None):
        """
        Instanciate gateway instances in the query plane
        See also manifold.core.query_plan
        Args:
            query_plan: A query plane instance, deduced from the Query
            user: (dict)
            timestamp: Timestamp (String instance) related to the Query
        """
        # XXX Platforms only serve for metadata
        # in fact we should initialize filters from the instance, then rely on
        # Storage including those filters...
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

                if platform.auth_type in ['none', 'default']:
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
            
            args = [self, name, None, platform.gateway_config, config, user]
            gw = Gateway.get(platform.gateway_name)(*args)

            gw.set_query(from_node.query)
            gw.query.timestamp = timestamp
            gw.set_identifier(from_node.get_identifier())
            from_node.set_gateway(gw)

    def get_metadata_objects(self):
        output = []
        # XXX Not generic
        for table in self.g_3nf.graph.nodes():
            fields = [f for f in table.get_fields()]
            fields = list(set(fields))

            # Build columns from fields
            columns = []
            for field in fields:
                column = {
                    'name'       : field.get_name(),
                    'qualifier'  : field.get_qualifier(),
                    'type'       : field.type,
                    'is_array'   : field.is_array(),
                    'description': field.get_description()
                    #"column"         : field.get_name(),        # field(_name)
                    #"description"    : field.get_description(), # description
                    #"header"         : field,
                    #"title"          : field,
                    #"unit"           : "N/A",                   # !
                    #"info_type"      : "N/A",
                    #"resource_type"  : "N/A",
                    #"value_type"     : "N/A",
                    #"allowed_values" : "N/A",
                    ## ----
                    #"type": field.type,                         # type
                    #"is_array"       : field.is_array(),        # array?
                    #"qualifier"      : field.get_qualifier()    # qualifier (const/RW)
                                                                # ? category == dimension
                }
                columns.append(column)

            # Add table metadata
            output.append({
                "table"  : table.get_name(),
                "column" : columns
            })
        return output

    def metadata_get_keys(self, table_name):
        return self.g_3nf.find_node(table_name).get_keys()

    def forward(self, query, deferred=False, execute=True, user=None):
        # Implements common functionalities = local queries, etc.
        namespace = None
        # Handling internal queries
        if ':' in query.object:
            namespace, table = query.object.rsplit(':', 2)

        if namespace == self.LOCAL_NAMESPACE:
            if table == 'object':
                output = self.get_metadata_objects()
            else:
                q = query.copy()
                q.object = table
                output =  Storage.execute(q, user=user)
            return ResultValue.get_success(output)
        elif namespace:
            raise Exception, "Unsupported namespace '%s'" % namespace
     
        # None is returned to inform child classes they are in charge of the answer
        return None
