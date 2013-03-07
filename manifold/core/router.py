import os, sys, json, copy, time, traceback #, threading
from types                          import StringTypes
from twisted.internet               import defer
from manifold.core.filter           import Predicate
from manifold.core.ast              import AST
from manifold.core.key              import Key, Keys
from manifold.core.query            import Query, AnalyzedQuery
from manifold.core.table            import Table
from manifold.gateways              import Gateway
from manifold.models                import DBPlatform as Platform, DBUser as User, DBAccount as Account, db
from manifold.core.dbnorm           import to_3nf 
from manifold.core.dbgraph          import DBGraph
from manifold.core.query_plan       import QueryPlan
from manifold.util.type             import returns, accepts
from manifold.gateways.sfa          import ADMIN_USER
from manifold.util.callback         import Callback
from manifold.core.interface        import Interface
from manifold.util.reactor_thread   import ReactorThread
# XXX cannot use the wrapper with sample script
# XXX cannot use the thread with xmlrpc -n
#from manifold.util.reactor_wrapper  import ReactorWrapper as ReactorThread

# TO BE REMOVED
from sfa.trust.credential           import Credential

CACHE_LIFETIME     = 1800

#------------------------------------------------------------------
# Class Router
# Router configured only with static/local routes, and which
# does not handle routing messages
#------------------------------------------------------------------

class Router(Interface):
    """
    Implements a TopHat router.

    Specialized to handle Announces/Routes, ...
    """

    def boot(self):
        #print "I: Booting router"
        # Install static routes in the RIB and FIB (TODO)
        #print "D: Reading static routes in: '%s'" % self.conf.STATIC_ROUTES_FILE
        #static_routes = self.fetch_static_routes(STATIC_ROUTES_FILE)
        #self.rib[dest] = route
        ReactorThread().start_reactor()
        # initialize dummy list of credentials to be uploaded during the
        # current session
        self.cache = {}

        super(Router, self).boot()

        self.build_tables()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        ReactorThread().stop_reactor()

    def add_credential(self, cred, platform, user):
        print "I: Adding credential to platform '%s' and user '%s'" % (platform, user.email)
        accounts = [a for a in user.accounts if a.platform.platform == platform]
        if not accounts:
            raise Exception, "Missing user account"
        account = accounts[0]
        config = account.config_get()


        if isinstance(cred, dict):
            cred = cred['cred']
        
        c = Credential(string=cred)
        c_type = c.get_gid_object().get_type()
        c_target = c.get_gid_object().get_hrn()
        delegate = c.get_gid_caller().get_hrn()

        # Get the account of the admin user in the database
        try:
            admin_user = db.query(User).filter(User.email == ADMIN_USER).one()
        except Exception, e:
            raise Exception, 'Missing admin user. %s' % str(e)

        # Get user account
        admin_accounts = [a for a in admin_user.accounts if a.platform.platform == platform]
        if not admin_accounts:
            raise Exception, "Accounts should be created for MySlice admin user"
        admin_account = admin_accounts[0]
        admin_config = admin_account.config_get()
        admin_user_hrn = admin_config['user_hrn']

        if account.auth_type != 'user':
            raise Exception, "Cannot upload credential to a non-user managed account."
        # Inspect admin account for platform
        if delegate != admin_user_hrn: 
            raise Exception, "Credential should be delegated to %s instead of %s" % (admin_user_hrn, delegate)

        if c_type == 'user':
            config['user_credential'] = cred
        elif c_type == 'slice':
            if not 'slice_credentials' in config:
                config['slice_credentials'] = {}
            config['slice_credentials'][c_target] = cred
        elif c_type == 'authority':
            config['authority_credential'] = cred
        else:
            raise Exception, "Invalid credential type"
        
        account.config_set(config)

        #print "USER: " + cred.get_gid_caller().get_hrn()
        #print "USER: " + cred.get_subject()
        #print "DELEGATED BY: " + cred.parent.get_gid_caller().get_hrn()
        #print "TARGET: " + cred.get_gid_object().get_hrn()
        #print "EXPIRATION: " + cred.get_expiration().strftime("%d/%m/%y %H:%M:%S")
        #delegate = cred.get_gid_caller().get_hrn()
        #if not delegate == 'ple.upmc.slicebrowser':
        #    raise PLCInvalidArgument, "Credential should be delegated to ple.upmc.slicebrowser instead of %s" % delegate;
        #cred_fields = {
        #    "credential_person_id" : self.caller["person_id"],
        #    "credential_target"    : cred.get_gid_object().get_hrn(),
        #    "credential_expiration": cred.get_expiration(),
        #    "credential_type"      : cred.get_gid_object().get_type(),
        #    "credential"           : cred.save_to_string()
        #}

    def cred_to_struct(self, cred):
        c = Credential(string = cred)
        return {
            "target"     : c.get_gid_object().get_hrn(),
            "expiration" : c.get_expiration(),
            "type"       : c.get_gid_object().get_type(),
            "credential" : c.save_to_string()
        }

    def get_credentials(self, platform, user):
        creds = []

        account = [a for a in user.accounts if a.platform.platform == platform][0]
        config = account.config_get()

        creds.append(self.cred_to_struct(config["user_credential"]))
        for sc in config["slice_credentials"].values():
            creds.append(self.cred_to_struct(sc))
        creds.append(self.cred_to_struct(config["authority_credential"]))

        return creds

    @returns(Keys)
    def metadata_get_keys(self, table_name):
        """
        \brief Retrieve the Key instances related to a Table announced
            in the metadata
        \param table_name The name of the table
        \return The Keys instance related to this table, None if
            table_name is not found in the metadata
        """
        for table in self.tables: #self.metadata_get_tables():
            if table.get_name() == table_name:
                return table.get_keys()
        return None

    # 1. NORMALIZATION
    def build_tables(self):
        """
        \brief Compute the 3nf schema according to the Tables
            announced in the metadata
        """
        # XXX Temporary: all announces are in MetadataClass format. Let's transform them to tables
        self.tables = []
        for platform, announces in self.metadata.items():
            for class_name, announce in announces.items():
                x = announce.table
                table = Table(platform, None, x.class_name, x.fields, x.keys)
                self.tables.append(table)

        self.g_3nf = to_3nf(self.tables)
        #self.g_3nf = to_3nf(self.metadata_get_tables())

    # This function is directly called for a Router
    # Decoupling occurs before for queries received through sockets
    def forward(self, query, deferred=False, execute=True, user=None):
        """
        A query is forwarded. Eventually it affects the forwarding plane, and expects an answer.
        NOTE : a query is like a flow
        """

        # Handling internal queries
        if ':' in query.fact_table:
            #try:
            namespace, table = query.fact_table.rsplit(':', 2)
            if namespace == self.LOCAL_NAMESPACE:
                q = copy.deepcopy(query)
                q.fact_table = table
                return self.local_query(q)
            elif namespace == "metadata":
                # Metadata are obtained for the 3nf representation in
                # memory
                if table == "table":
                    output = []
                    # XXX Not generic
                    for table in self.G_nf.graph.nodes():
                        #print "GNF table", table
                        fields = [f for f in self.G_nf.get_fields(table)]
                        fields = list(set(fields))

                        # Build columns from fields
                        columns = []
                        for field in fields:
                            column = {
                                "column"         : field.get_name(),        # field(_name)
                                "description"    : field.get_description(), # description
                                "header"         : field,
                                "title"          : field,
                                "unit"           : "N/A",                   # !
                                "info_type"      : "N/A",
                                "resource_type"  : "N/A",
                                "value_type"     : "N/A",
                                "allowed_values" : "N/A",
                                # ----
                                "type": field.type,                         # type
                                "is_array"       : field.is_array(),        # array?
                                "qualifier"      : field.get_qualifier()    # qualifier (const/RW)
                                                                            # ? category == dimension
                            }
                            columns.append(column)

                        # Add table metadata
                        output.append({
                            "table"  : table.get_name(),
                            "column" : columns
                        })
                    return output
                else:
                    raise Exception, "Unsupported metadata request '%s'" % table
            else:
                raise Exception, "Unsupported namespace '%s'" % namespace
        route = None


        return self.do_forward(query, route, deferred, execute, user)
            
    def do_forward(self, query, route, deferred, execute=True, user=None):
        """
        Effectively runs the forwarding of the query to the route
        """

        # the route parameter is ignored until we clearly state what are the
        # different entities of a router and their responsabilities



        if not execute: 
            qp = QueryPlan()
            qp.build(query, self.g_3nf, self.allowed_capabilities, user)

            print ""
            print "QUERY PLAN:"
            print "-----------"
            qp.dump()
            print ""
            print ""

            return None

        # The query plan will be the same whatever the action: it represents
        # the easier way to reach the destination = routing
        # We do not need the full query for the query plan, in fact just the
        # destination, which is a subpart of the query = (fact, filters, fields)
        # action = what to do on this QP
        # ts = how it behaves

        # Caching ?
        try:
            h = hash((user,query))
            #print "ID", h, ": looking into cache..."
        except:
            h = 0

        if query.get_action() == "get":
            if h != 0 and h in self.cache:
                res, ts = self.cache[h]
                print "Cache hit!"
                if ts > time.time():
                    return res
                else:
                    print "Expired entry!"
                    del self.cache[h]

        # Building query plan
        qp = QueryPlan()
        qp.build(query, self.g_3nf, self.allowed_capabilities, user)

        #d = defer.Deferred() if deferred else None
        #cb = Callback(d, router=self, cache_id=h)
        #qp.callback = cb

        if query.get_action() == "update":
            # At the moment we can only update if the primary key is present
            keys = self.metadata_get_keys(query.get_from())
            if not keys:
                raise Exception, "Missing metadata for table %s" % query.get_from()
            key = list(keys).pop()
            
            if not query.filters.has_eq(key):
                raise Exception, "The key field '%s' must be present in update request" % key

        return qp.execute()
