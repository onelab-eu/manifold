import os, sys, json, copy, time, traceback #, threading
from types                          import StringTypes
from twisted.internet               import defer
from manifold.core.filter           import Predicate
from manifold.core.ast              import AST
from manifold.core.key              import Key, Keys
from manifold.core.query            import Query, AnalyzedQuery
from manifold.core.table            import Table
from manifold.gateways              import Gateway
from manifold.models                import *
from manifold.core.dbnorm           import to_3nf 
from manifold.core.dbgraph          import DBGraph
from manifold.core.query_plan       import QueryPlan
from manifold.util.type             import returns, accepts
from manifold.gateways.sfa          import ADMIN_USER
from manifold.util.callback         import Callback
from manifold.core.interface        import Interface
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.storage          import DBStorage as Storage
from manifold.core.result_value     import ResultValue

from manifold.core.announce         import Announces
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
        # initialize dummy list of credentials to be uploaded during the
        # current session
        self.cache = {}
        
        super(Router, self).boot()
        
        #self.g_3nf = to_3nf(self.tables)
        self.g_3nf = to_3nf(self.metadata)
#DEPRECATED#        self.build_tables()
        #print "****** ANNOUNCES ******"
        #for a in Announces.get_announces(self.g_3nf):
        #    print a

    def __enter__(self):
        ReactorThread().start_reactor()
        return self

    def __exit__(self, type, value, traceback):
        ReactorThread().stop_reactor()

#DEPRECATED#    def add_credential(self, cred, platform, user):
#DEPRECATED#        print "I: Adding credential to platform '%s' and user '%s'" % (platform, user.email)
#DEPRECATED#        accounts = [a for a in user.accounts if a.platform.platform == platform]
#DEPRECATED#        if not accounts:
#DEPRECATED#            raise Exception, "Missing user account"
#DEPRECATED#        account = accounts[0]
#DEPRECATED#        config = account.config_get()
#DEPRECATED#
#DEPRECATED#
#DEPRECATED#        if isinstance(cred, dict):
#DEPRECATED#            cred = cred['cred']
#DEPRECATED#        
#DEPRECATED#        c = Credential(string=cred)
#DEPRECATED#        c_type = c.get_gid_object().get_type()
#DEPRECATED#        c_target = c.get_gid_object().get_hrn()
#DEPRECATED#        delegate = c.get_gid_caller().get_hrn()
#DEPRECATED#
#DEPRECATED#        # Get the account of the admin user in the database
#DEPRECATED#        try:
#DEPRECATED#            admin_user = db.query(User).filter(User.email == ADMIN_USER).one()
#DEPRECATED#        except Exception, e:
#DEPRECATED#            raise Exception, 'Missing admin user. %s' % str(e)
#DEPRECATED#
#DEPRECATED#        # Get user account
#DEPRECATED#        admin_accounts = [a for a in admin_user.accounts if a.platform.platform == platform]
#DEPRECATED#        if not admin_accounts:
#DEPRECATED#            raise Exception, "Accounts should be created for MySlice admin user"
#DEPRECATED#        admin_account = admin_accounts[0]
#DEPRECATED#        admin_config = admin_account.config_get()
#DEPRECATED#        admin_user_hrn = admin_config['user_hrn']
#DEPRECATED#
#DEPRECATED#        if account.auth_type != 'user':
#DEPRECATED#            raise Exception, "Cannot upload credential to a non-user managed account."
#DEPRECATED#        # Inspect admin account for platform
#DEPRECATED#        if delegate != admin_user_hrn: 
#DEPRECATED#            raise Exception, "Credential should be delegated to %s instead of %s" % (admin_user_hrn, delegate)
#DEPRECATED#
#DEPRECATED#        if c_type == 'user':
#DEPRECATED#            config['user_credential'] = cred
#DEPRECATED#        elif c_type == 'slice':
#DEPRECATED#            if not 'slice_credentials' in config:
#DEPRECATED#                config['slice_credentials'] = {}
#DEPRECATED#            config['slice_credentials'][c_target] = cred
#DEPRECATED#        elif c_type == 'authority':
#DEPRECATED#            config['authority_credential'] = cred
#DEPRECATED#        else:
#DEPRECATED#            raise Exception, "Invalid credential type"
#DEPRECATED#        
#DEPRECATED#        account.config_set(config)
#DEPRECATED#
#DEPRECATED#        #print "USER: " + cred.get_gid_caller().get_hrn()
#DEPRECATED#        #print "USER: " + cred.get_subject()
#DEPRECATED#        #print "DELEGATED BY: " + cred.parent.get_gid_caller().get_hrn()
#DEPRECATED#        #print "TARGET: " + cred.get_gid_object().get_hrn()
#DEPRECATED#        #print "EXPIRATION: " + cred.get_expiration().strftime("%d/%m/%y %H:%M:%S")
#DEPRECATED#        #delegate = cred.get_gid_caller().get_hrn()
#DEPRECATED#        #if not delegate == 'ple.upmc.slicebrowser':
#DEPRECATED#        #    raise PLCInvalidArgument, "Credential should be delegated to ple.upmc.slicebrowser instead of %s" % delegate;
#DEPRECATED#        #cred_fields = {
#DEPRECATED#        #    "credential_person_id" : self.caller["person_id"],
#DEPRECATED#        #    "credential_target"    : cred.get_gid_object().get_hrn(),
#DEPRECATED#        #    "credential_expiration": cred.get_expiration(),
#DEPRECATED#        #    "credential_type"      : cred.get_gid_object().get_type(),
#DEPRECATED#        #    "credential"           : cred.save_to_string()
#DEPRECATED#        #}
#DEPRECATED#
#DEPRECATED#    def cred_to_struct(self, cred):
#DEPRECATED#        c = Credential(string = cred)
#DEPRECATED#        return {
#DEPRECATED#            "target"     : c.get_gid_object().get_hrn(),
#DEPRECATED#            "expiration" : c.get_expiration(),
#DEPRECATED#            "type"       : c.get_gid_object().get_type(),
#DEPRECATED#            "credential" : c.save_to_string()
#DEPRECATED#        }
#DEPRECATED#
#DEPRECATED#    def get_credentials(self, platform, user):
#DEPRECATED#        creds = []
#DEPRECATED#
#DEPRECATED#        account = [a for a in user.accounts if a.platform.platform == platform][0]
#DEPRECATED#        config = account.config_get()
#DEPRECATED#
#DEPRECATED#        creds.append(self.cred_to_struct(config["user_credential"]))
#DEPRECATED#        for sc in config["slice_credentials"].values():
#DEPRECATED#            creds.append(self.cred_to_struct(sc))
#DEPRECATED#        creds.append(self.cred_to_struct(config["authority_credential"]))
#DEPRECATED#
#DEPRECATED#        return creds

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

#DEPRECATED#    # 1. NORMALIZATION
#DEPRECATED#    def build_tables(self):
#DEPRECATED#        """
#DEPRECATED#        \brief Compute the 3nf schema according to the Tables
#DEPRECATED#            announced in the metadata
#DEPRECATED#        """
#DEPRECATED#        # XXX Temporary: all announces are in MetadataClass format. Let's transform them to tables
#DEPRECATED#        self.tables = []
#DEPRECATED#        for platform, announces in self.metadata.items():
#DEPRECATED#            # XXX how is self.metadata constructed ???
#DEPRECATED#            for class_name, announce in announces.items():
#DEPRECATED#                #x = announce.table
#DEPRECATED#                #table = Table(platform, None, x.class_name, x.fields, x.keys)
#DEPRECATED#                #self.tables.append(table)
#DEPRECATED#                self.tables.append(announce.table)
#DEPRECATED#
#DEPRECATED#        self.g_3nf = to_3nf(self.tables)
#DEPRECATED#        #self.g_3nf = to_3nf(self.metadata_get_tables())

    # This function is directly called for a Router
    # Decoupling occurs before for queries received through sockets
    def forward(self, query, deferred=False, execute=True, user=None):
        """
        A query is forwarded. Eventually it affects the forwarding plane, and expects an answer.
        NOTE : a query is like a flow
        """
        ret = super(Router, self).forward(query, deferred, execute, user)
        if ret: return ret

        # We suppose we have no namespace from here
        if not execute: 
            qp = QueryPlan()
            qp.build(query, self.g_3nf, self.allowed_capabilities, user)

            print ""
            print "QUERY PLAN:"
            print "-----------"
            qp.dump()
            print ""
            print ""

            return ResultValue.get_success(None)

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

        print ""
        print "QUERY PLAN:"
        print "-----------"
        qp.dump()
        print ""
        print ""

        #d = defer.Deferred() if deferred else None
        #cb = Callback(d, router=self, cache_id=h)
        #qp.callback = cb

        self.instanciate_gateways(qp, user)

        if query.get_action() == "update":
            # At the moment we can only update if the primary key is present
            keys = self.metadata_get_keys(query.get_from())
            if not keys:
                raise Exception, "Missing metadata for table %s" % query.get_from()
            key = list(keys).pop()
            
            if not query.filters.has_eq(key):
                raise Exception, "The key field '%s' must be present in update request" % key

        results = qp.execute()
        return ResultValue.get_result_value(results, qp.get_result_value_array())
