import os, sys, json, time, traceback #, threading
from types                        import StringTypes

from twisted.internet             import defer

from manifold.core.filter         import Predicate
from manifold.core.ast            import AST
from manifold.core.key            import Key, Keys
from manifold.core.query          import Query
from manifold.core.table          import Table
from manifold.gateways            import Gateway
from manifold.models              import *
from manifold.core.dbnorm         import to_3nf 
from manifold.core.dbgraph        import DBGraph
from manifold.util.dfs            import dfs
from manifold.core.pruned_tree    import build_pruned_tree
from manifold.core.query_plan    import build_query_plan
from manifold.util.reactor_thread import ReactorThread
from manifold.util.type           import returns, accepts
from manifold.gateways.sfa        import ADMIN_USER
from manifold.metadata.Metadata   import import_file_h
from manifold.util.callback       import Callback

from sfa.trust.credential         import Credential

#import copy
#import time
#import random
#import base64
#
#from sqlalchemy.sql          import operators
#
#from tophat.router.conf      import Conf
#from tophat.router.rib       import RIB
#from tophat.router.fib       import FIB
#from tophat.router.flowtable import FlowTable
#from manifold.models           import *
#from manifold.util.misc        import get_sqla_filters, xgetattr

STATIC_ROUTES_FILE = "/usr/share/myslice/metadata/"
CACHE_LIFETIME     = 1800

#------------------------------------------------------------------
# Class Destination
# Represent the destination (a query in our case) of a TopHat route
#------------------------------------------------------------------
#
#class Destination(Destination, Query):
#    """
#    Implements a destination in TopHat == a view == a query
#    """
#    
#    def __str__(self):
#        return "<Destination / Query: %s" % self.query

#------------------------------------------------------------------
# Class Route
#------------------------------------------------------------------

class Route(object):
    """
    Implements a TopHat route.
    """

    pass
#
#    def __init__(self, destination, peer, cost, timestamp):
#
#        if type(destination) != Destination:
#            raise TypeError("Destination of type %s expected in argument. Got %s" % (type(destination), Destination))
#
#        # Assert the route corresponds to an existing peer
#        # Assert the cost corresponds to a right cost
#        # Eventually the timestamp would not be a parameter but assigned
#
#        super(Route, self).__init__(self, destination, peer, cost, timestamp)
#
#    def push(identifier, record):
#        pass

#------------------------------------------------------------------
# Class Cost
# Cost related to a route in the routing table
#------------------------------------------------------------------
#
#class Cost(int):
#    """
#    Let's use (N, min, +) semiring for cost
#    """
#
#    def __add__(self, other):
#        return Cost(min(self, other))
#
#    def __mul__(self, other):
#        return Cost(self + other)

#------------------------------------------------------------------
# Class Router
# Router configured only with static/local routes, and which
# does not handle routing messages
#------------------------------------------------------------------

class Router(object):
    """
    Implements a TopHat router.

    Specialized to handle Announces/Routes, ...
    """

    def __init__(self):
        print "I: Init Router" 
        self.reactor = ReactorThread()

        #self.route_cls = route_cls
        #self.conf = Conf()
        self.rib = {} #RIB(dest_cls, Route)
        #self.fib = FIB(Route)
        #self.flow_table = FlowTable(route_cls)
        self.boot()

        # account.manage()

        # XXX we insert a dummy platform
        #p = Platform(platform = 'mytestbed', platform_longname='MyTestbed')
        #db.add(p) 
        #p = Platform(platform = 'tophat', platform_longname='TopHat')
        #db.add(p) 

        # initialize dummy list of credentials to be uploaded during the
        # current session
        self.cache = {}

    def boot(self):
        #print "I: Booting router"
        # Install static routes in the RIB and FIB (TODO)
        #print "D: Reading static routes in: '%s'" % self.conf.STATIC_ROUTES_FILE
        static_routes = self.fetch_static_routes(STATIC_ROUTES_FILE)
        #self.rib[dest] = route
        self.build_tables()

        # Read peers into the configuration file
        # TODO

    def __enter__(self):
        self.reactor.startReactor()
        return self

    def __exit__(self, type, value, traceback):
        self.reactor.stopReactor()
        self.reactor.join()

    def import_file_h(self, directory, platform, gateway_type):
        """
        \brief Import a .h file (see manifold.metadata/*.h)
        \param directory The directory storing the .h files
            Example: router.conf.STATIC_ROUTES_FILE = "/usr/share/myslice/metadata/"
        \param platform The name of the platform we are configuring
            Examples: "ple", "senslab", "tophat", "omf", ...
        \param gateway_types The type of the gateway
            Examples: "SFA", "XMLRPC", "MaxMind"
            See:
                sqlite3 /var/myslice/db.sqlite
                > select gateway_type from platform;
        """
        # Check path
        filename = os.path.join(directory, "%s.h" % gateway_type)
        if not os.path.exists(filename):
            filename = os.path.join(directory, "%s-%s.h" % (gateway_type, platform))
            if not os.path.exists(filename):
                raise Exception, "Metadata file '%s' not found (platform = %r, gateway_type = %r)" % (filename, platform, gateway_type)

        # Read input file
        routes = []
        print "I: Platform %s: Processing %s" % (platform, filename)
        (classes, enums) = import_file_h(filename)

        # Check class consistency
        for cur_class_name, cur_class in classes.items():
            invalid_keys = cur_class.get_invalid_keys()
            if invalid_keys:
                raise ValueError("In %s: in class %r: key(s) not found: %r" % (filename, cur_class_name, invalid_keys))

        # Rq: We cannot check type consistency while a table might refer to types provided by another file.
        # Thus we can't use get_invalid_types yet

        # Feed RIB
        for cur_class_name, cur_class in classes.items():
            t = Table(platform, None, cur_class_name, cur_class.fields, cur_class.keys) # None = methods
            self.rib[t] = platform
        return routes

    def get_gateway(self, platform, query, user):
        # XXX Ideally, some parameters regarding MySlice user account should be
        # stored outside of the platform table

        # Finds the gateway corresponding to the platform
        if isinstance(platform, (tuple, list, set, frozenset)):
            if len(list(platform)) > 1:
                print "W: get_gateway: only keeping the first platform in %s" % platform
            platform = list(platform)[0]

        try:
            p = db.query(Platform).filter(Platform.platform == platform).one()
        except Exception, e:
            raise Exception, "E: Missing gateway information for platform '%s': %s" % (platform, e)

        # Get the corresponding class
        gtype = p.gateway_type.encode('latin1')
        gw = Gateway.get(gtype)
        #try:
        #    gw = getattr(__import__('tophat.gateways', globals(), locals(), gtype), gtype)
        #except Exception, e:
        #    raise Exception, "E: Cannot import gateway class '%s': %s" % (gtype, e)

        # Gateway config
        gconf = json.loads(p.config)

        # Get user account
        accounts = [a for a in user.accounts if a.platform.platform == platform]
        if not accounts:
            # No user account for platform, let's skip it
            print "E: No user account for platform : %s" % platform
            account = None 
            return gw(self, platform, query, gconf, None, user)
            #account = Account(user=user, platform=p, auth_type='managed', config='{}')
            #db.add(account)
            #db.commit()
        else:
            account = accounts[0]
        

        # User account config
        if account.auth_type == 'reference':
            ref_platform = json.loads(account.config)['reference_platform']
            ref_accounts = [a for a in user.accounts if a.platform.platform == ref_platform]
            if not ref_accounts:
                raise Exception, "reference account does not exist"
            ref_account = ref_accounts[0]
            aconf = ref_account.config
        else:
            aconf = account.config
        aconf = json.loads(aconf)

#        if account.auth_type == 'reference':
#            reference_platform = json.loads(account.config)['reference_platform']
#            ref_accounts = [a for a in user.accounts if a.platform.platform == reference_platform]
#            if not ref_accounts:
#                raise Exception, "reference account does not exist"
#            ref_account = ref_accounts[0]
#            aconf = json.loads(ref_account.config) if account else None
#            if aconf: aconf['reference_platform'] = reference_platform
#        else:
#            aconf = json.loads(account.config) if account else None

        return gw(self, platform, query, gconf, aconf, user)

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

    @returns(list)
    def metadata_get_tables(self):
        """
        \return The list of Table instances announced in the metadata
            collected by this router
        """
        return self.rib.keys() # HUM

    @returns(Keys)
    def metadata_get_keys(self, table_name):
        """
        \brief Retrieve the Key instances related to a Table announced
            in the metadata
        \param table_name The name of the table
        \return The Keys instance related to this table, None if
            table_name is not found in the metadata
        """
        for table in self.metadata_get_tables():
            if table.get_name() == table_name:
                return table.get_keys()
        return None

    def build_tables(self):
        """
        \brief Compute the 3nf schema according to the Tables
            announced in the metadata
        """
        self.g_3nf = to_3nf(self.metadata_get_tables())

    def fetch_static_routes(self, directory = STATIC_ROUTES_FILE):
        """
        \brief Retrieve static routes related to each plaform. 
            See:
                sqlite3 /var/myslice/db.sqlite
                > select platform, gateway_type from platform;
        \param directory Containing static route files. 
        \return The corresponding static routes
        """
        routes = []

        # Query the database to retrieve which configuration file has to be
        # loaded for each platform. 
        platforms = db.query(Platform).filter(Platform.disabled == False).all()

        # For each platform, load the corresponding .h file
        for platform in platforms:
            gateway = None
            try:
                tables = self.import_file_h(
                    directory,
                    platform.platform,
                    platform.gateway_type
                )
            except Exception, why:
                print "Error while importing %s in get_static_routes: %s" % (platform, why)
            
            routes.extend(tables)

        return routes

#OBSOLETE|    def get_platform_max_fields(self, fields, join):
#OBSOLETE|        # Search for the platform::method that allows for the largest number of missing fields
#OBSOLETE|        _fields = [f.split(".")[0] for f in fields]
#OBSOLETE|        maxfields = 0
#OBSOLETE|        ret = (None, None)
#OBSOLETE|        
#OBSOLETE|        for dest, route in self.rib.items():
#OBSOLETE|            # HACK to make tophat on join
#OBSOLETE|            if not join and dest.platform in ["tophat", "myslice"]:
#OBSOLETE|                continue
#OBSOLETE|            isect = set(dest.fields).intersection(set(_fields))
#OBSOLETE|            if len(isect) > maxfields:
#OBSOLETE|                maxfields = len(isect)
#OBSOLETE|                ret = (dest, isect)
#OBSOLETE|        return ret

    def process_subqueries(self, query, user):
        """
        \brief Compute the AST (tree of SQL operators) related to a query
        \sa manifold.core.ast.py
        \param query A Query issued by the user
        \param user A User instance (carry user's information) 
        \return An AST instance representing the query plan related to the query
        """
        print "=" * 100
        print "Entering process_subqueries %s (need fields %s) " % (query.get_from(), query.get_select())
        print "=" * 100
        table_name = query.get_from()
        table = self.g_3nf.find_node(table_name)
        if not table:
            raise ValueError("Can't find table %r related to query %r" % (table_name, query))

        qp = AST(user)

        cur_filters = []
        cur_params = {}
        cur_fields = []
        subq = {}

        debug = "debug" in query.get_params() and query.get_params()["debug"]

        # XXX there are some parameters that will be answered by the parent !!!! no need to request them from the children !!!!
        # XXX XXX XXX XXX XXX XXX ex slice.resource.PROPERTY

        if query.filters:
            for pred in query.filters:
                if "." in pred.key:
                    method, subkey = pred.key.split(".", 1)
                    if not method in subq:
                        subq[method] = {}
                    if not "filters" in subq[method]:
                        subq[method]["filters"] = []
                    subq[method]["filters"].append(Predicate(subkey, pred.op, pred.value))
                else:
                    cur_filters.append(pred)

        if query.params:
            for key, value in query.params.items():
                if "." in key:
                    method, subkey = key.split(".", 1)
                    if not method in subq:
                        subq[method] = {}
                    if not "params" in subq[method]:
                        subq[method]["params"] = {}
                    subq[method]["params"][subkey, value]
                else:
                    cur_params[key] = value

        if query.fields:
            for field in query.fields:
                if "." in field:
                    method, subfield = field.split(".", 1)
                    if not method in subq:
                        subq[method] = {}
                    if not "fields" in subq[method]:
                        subq[method]["fields"] = []
                    subq[method]["fields"].append(subfield)
                else:
                    cur_fields.append(field)

        if len(subq):
            children_ast = []
            for method, subquery in subq.items():
                # We need to add the keys of each subquery
                # We append the method name (eg. resources) which should return the list of keys
                # (and eventually more information, but they will be ignored for the moment)

                method = table.get_field(method).get_type()
                if not method in cur_fields:
                    cur_fields.append(method)

                # Recursive construction of the processed subquery
                subfilters = subquery["filters"] if "filters" in subquery else []
                subparams  = subquery["params"]  if "params"  in subquery else {}
                subfields  = subquery["fields"]  if "fields"  in subquery else []
                subts      = query.get_ts()

                print "method     = ", method
                print "subfilters = ", subfilters
                print "subparams  = ", subparams 
                print "subfields  = ", subfields 

                # Adding primary key in subquery to be able to merge
                keys = self.metadata_get_keys(method)
                if keys:
                    key = list(keys).pop()
                    print "W: selecting arbitrary key %s to join with '%s'" % (key, method)
                    if isinstance(key, Key):
                        for field in key:
                            field_name = field.get_name()
                            if field_name not in subfields:
                                subfields.append(field_name)
                    else:
                        raise TypeError("Invalid type: key = %s (type %s)" % (key, type(key)))

                # XXX Adding subfields either requested by the users or
                # necessary for the join

                # NOTE: when requesting fields from a subquery, there
                # are several possibilities:
                # 1 - only keys are returned
                # 2 - fields are returned but we cannot predict
                # 3 - we have a list of fields that can be returned
                # (default)
                # 4 - all fields can be returned
                # BTW can we specify which fields we want to force the
                # platform to do most of the work for us ?
                #
                # To begin with, let's only consider case 1 and 4
                # XXX where to get this information in metadata
                # XXX case 2 could be handled by injection (we inject
                # fields before starting, and if we have all required
                # fields, we can return directly).
                # XXX case 3 could be a special case of 4

                # We have two solutions:
                # 1) build the whole child ast (there might be several
                # solutions and one will be chosen) then inject the
                # results we already have (we might be able to inject
                # more in a non chosen solution maybe ?? or maybe not
                # since we are in 3nf)
                # 2) build the child ast considering that we have
                # already a set of fields
                # 
                # Let's start with solution 1) since it might be more
                # robust in the current state given we don't have an
                # exact idea of what will be the returned fields.

                # Formulate the query we are trying to resolve
                print "Preparing subquery on", method

                if debug:
                    subparams["debug"] = True

                subquery = Query(query.get_action(), method, subfilters, subparams, subfields, subts)

                child_ast = self.process_subqueries(subquery, user)
                children_ast.append(child_ast.root)

            parent = Query(query.get_action(), query.get_from(), cur_filters, cur_params, cur_fields, query.get_ts())
            parent_ast = self.process_query(parent, user)
            qp = parent_ast
            qp.subquery(children_ast)
        else:
            parent = Query(query.get_action(), query.get_from(), cur_filters, cur_params, cur_fields, query.get_ts())
            qp = self.process_query(parent, user)

        return qp

#OBSOLETE|    def get_table_max_fields(fields, tables):
#OBSOLETE|        maxfields = 0
#OBSOLETE|        ret = (None, None)
#OBSOLETE|        for t in tables:
#OBSOLETE|            isect = set(fields).intersection(t.fields)
#OBSOLETE|            if len(isect) > maxfields:
#OBSOLETE|                maxfields = len(isect)
#OBSOLETE|                ret = (t, isect)
#OBSOLETE|        return ret

    def get_query_plan(self, query, user):
        """
        \brief Compute the query plan related to a user's query
        \param query The Query issued by the user
        \param user A User instance
        \return An AST instance
        """
        qp = self.process_subqueries(query, user)

        # Now we apply the operators
        #qp = qp.selection(query.filters) 
        #qp = qp.projection(query.fields) 
        #qp = qp.sort(query.get_sort()) 
        #qp = qp.limit(query.get_limit()) 

        # We should now have a query plan
        print ""
        print "QUERY PLAN:"
        print "-----------"
        qp.dump()
        print ""
        print ""

        return qp

    #---------------
    # Local queries
    #---------------

    def local_query_get(self, query):
        #
        # XXX How are we handling subqueries
        #

        fields = query.fields
        # XXX else tap into metadata

        cls = self._map_local_table[query.fact_table]

        # Transform a Filter into a sqlalchemy expression
        _filters = get_sqla_filters(cls, query.filters)
        _fields = xgetattr(cls, query.fields) if query.fields else None

        if query.fields:
            res = db.query( *_fields ).filter(_filters)
        else:
            res = db.query( cls ).filter(_filters)

        tuplelist = res.all()
        # only 2.7+ table = [ { fields[idx] : val for idx, val in enumerate(t) } for t in tuplelist]
        table = [ dict([(fields[idx], val) for idx, val in enumerate(t)]) for t in tuplelist]
        return table

    def local_query_update(self, query):

        cls = self._map_local_table[query.fact_table]

        _fields = xgetattr(cls, query.fields)
        _filters = get_sqla_filters(cls, query.filters)
        # only 2.7+ _params = { getattr(cls, k): v for k,v in query.params.items() }
        _params = dict([ (getattr(cls, k), v) for k,v in query.params.items() ])

        #db.query(cls).update(_params, synchronize_session=False)
        db.query(cls).filter(_filters).update(_params, synchronize_session=False)
        db.commit()

        return []

    def local_query_create(self, query):

        assert not query.filters, "Filters should be empty for a create request"
        #assert not query.fields, "Fields should be empty for a create request"


        cls = self._map_local_table[query.fact_table]
        params = cls.process_params(query.params)
        new_obj = cls(**params)
        db.add(new_obj)
        db.commit()
        
        return []

    def local_query(self, query):
        _map_action = {
            "get"    : self.local_query_get,
            "update" : self.local_query_update,
            "create" : self.local_query_create
        }
        return _map_action[query.action](query)

    @returns(AST)
    def process_query(self, query, user):
        return self.process_query_mando(query, user)

    @returns(AST)
    def process_query_mando(self, query, user):
        """
        \brief Compute the query plan related to a query which involves
            no sub-queries. Sub-queries should already processed thanks to
            process_subqueries().
        \param query The Query instance representing the query issued by the user.
            \sa manifold/core/query.py
        \param user The User instance reprensenting the user issuing
            the query. The query can be resolved in various way according to
            the user grants.
            \sa tophat/model/user.py
        \return The AST instance representing the query plan.
        """

        # Compute the fields involved explicitly in the query (e.g. in SELECT or WHERE)
        needed_fields = set(query.get_select())
        if needed_fields == set():
            raise ValueError("No queried field")
        needed_fields.update(query.get_where().keys())

        # Retrieve the root node corresponding to the fact table
        root = self.g_3nf.find_node(query.get_from())

        # Retrieve the (unique due to 3-nf) tree included in "self.g_3nf" and rooted in "root"
        # \sa manifold.util.dfs.py
        print "Entering DFS(%r) in graph:" % root

        # Compute the corresponding pruned tree.
        # Each node of the pruned tree only gathers relevant table, and only their
        # relevant fields and their relevant key (if used).
        # \sa manifold.util.pruned_graph.py
        pruned_tree = build_pruned_tree(self.g_3nf.graph, needed_fields, dfs(self.g_3nf.graph, root))

        # Compute the skeleton resulting query plan
        # (e.g which does not take into account the query)
        # It leads to a query plan made of Union, From, and LeftJoin nodes
        return build_query_plan(user, query, pruned_tree)

#OBSOLETE|    def process_query_new(self, query, user):
#OBSOLETE|        """
#OBSOLETE|        \brief Compute the query plan related to a query which involves
#OBSOLETE|            no sub-queries. Sub-queries should already processed thanks to
#OBSOLETE|            process_subqueries().
#OBSOLETE|        \param query The query issued by the user.
#OBSOLETE|        \param user The user issuing the query (according to the user grants
#OBSOLETE|            the query plan might differ).
#OBSOLETE|        \return The AST instance representing the query plan.
#OBSOLETE|        """
#OBSOLETE|        # Find a tree of tables rooted at the fact table (e.g. method) included
#OBSOLETE|        # in the normalized graph.
#OBSOLETE|        root = self.g_3nf.find_node(query.get_from())
#OBSOLETE|        tree = [arc for arc in self.g_3nf.get_tree_edges(root)] # DFS tree 
#OBSOLETE|
#OBSOLETE|        # Compute the fields involved explicitly in the query (e.g. in SELECT or WHERE)
#OBSOLETE|        needed_fields = set(query.fields)
#OBSOLETE|        if query.filters:
#OBSOLETE|            needed_fields.update(query.filters.keys())
#OBSOLETE|
#OBSOLETE|        # Prune this tree to remove table that are not required to answer the query.
#OBSOLETE|        # As a consequence, each leave of the tree provides at least one queried field.
#OBSOLETE|        tree = DBGraph.prune_tree(tree, dict(self.g_3nf.graph.nodes(True)), needed_fields)
#OBSOLETE|
#OBSOLETE|        # TODO check whether every needed_fields is provided by a node of the tree
#OBSOLETE|        # We might only be able to partially answer a query. Inform the user
#OBSOLETE|        # about it
#OBSOLETE|
#OBSOLETE|        # Initialize a query plan
#OBSOLETE|        qp = AST(user)
#OBSOLETE|
#OBSOLETE|        # Process the root node
#OBSOLETE|        successors = self.g_3nf.get_successors(root)
#OBSOLETE|        print "W: Selecting an arbitrary key for each successors %r" % successors
#OBSOLETE|        succ_keys = [list(iter(table.get_keys()).next())[0] for table in successors]
#OBSOLETE|        current_fields = set(needed_fields) & set([field.get_name() for field in root.get_fields()])
#OBSOLETE|        current_fields |= set(succ_keys)
#OBSOLETE|        q = Query(
#OBSOLETE|            action     = query.get_action(),
#OBSOLETE|            fact_table = root.name,
#OBSOLETE|            filters    = None, #query.filters,
#OBSOLETE|            params     = None, #query.params,
#OBSOLETE|            fields     = list(current_fields),
#OBSOLETE|            ts         = query.ts
#OBSOLETE|        )
#OBSOLETE|        qp = qp.From(root, q)
#OBSOLETE|
#OBSOLETE|        needed_fields -= current_fields
#OBSOLETE|
#OBSOLETE|        # (tree == None) means the tree is reduced to the root node. We are done
#OBSOLETE|        if not tree:
#OBSOLETE|            if needed_fields: 
#OBSOLETE|                print "E: Unresolved fields", needed_fields
#OBSOLETE|            return qp
#OBSOLETE|
#OBSOLETE|        # XXX Can be optimized by parallelizing...
#OBSOLETE|        # XXX Note: not all arcs are JOIN, some are just about adding fields, or
#OBSOLETE|        # better choosing the platform.
#OBSOLETE|        for _, node in self.g_3nf.get_edges():
#OBSOLETE|            # Key is necessary for joining with the parent
#OBSOLETE|            # NOTE we will need to remember which key was collected
#OBSOLETE|            # maybe because it will be left in needed keys ???
#OBSOLETE|            key = iter(node.keys).next()
#OBSOLETE|            if isinstance(key, tuple): key = key[0]
#OBSOLETE|
#OBSOLETE|
#OBSOLETE|            # We need to ask to the local table at least one key for each
#OBSOLETE|            # successor XXX let's suppose for now there is only one key
#OBSOLETE|            # XXX in case of compound keys, what to do ?
#OBSOLETE|            successors = self.g_3nf.get_successors(node)
#OBSOLETE|            succ_keys = [iter(s.keys).next() for s in successors]
#OBSOLETE|
#OBSOLETE|            # Realize a left join ( XXX only for PROVIDES arcs)
#OBSOLETE|            current_fields = set(needed_fields) & set([field.get_name() for field in node.get_fields()])
#OBSOLETE|            print "succ_keys = ", succ_keys 
#OBSOLETE|            current_fields |= set(succ_keys)
#OBSOLETE|            current_fields.add(key) 
#OBSOLETE|
#OBSOLETE|            print "current_fields = ", current_fields
#OBSOLETE|            q = Query(
#OBSOLETE|                action     = query.get_action(),
#OBSOLETE|                fact_table = node.name,
#OBSOLETE|                filters    = None, #query.filters,
#OBSOLETE|                params     = None, # query.params,
#OBSOLETE|                fields     = list(current_fields),
#OBSOLETE|                ts         = query.ts
#OBSOLETE|            )
#OBSOLETE|
#OBSOLETE|            qp = qp.join(AST(user).From(node, q), key)
#OBSOLETE|
#OBSOLETE|            needed_fields -= current_fields
#OBSOLETE|        
#OBSOLETE|        if needed_fields:
#OBSOLETE|            print "E: Unresolved fields", needed_fields
#OBSOLETE|        return qp
#OBSOLETE|
#OBSOLETE|        # Explore the tree to compute which fields must be retrieved for each node/table
#OBSOLETE|        # - A child node can be explored if and only if one of its key is provided
#OBSOLETE|        # by the parent node (to join both table), so we need to retrieve the
#OBSOLETE|        # corresponding fields both in the parent and child.
#OBSOLETE|        # - Whenever a node is crossed, we compute if it provides some queried
#OBSOLETE|        # fields. If so, we also retrieve those fields
#OBSOLETE|
#OBSOLETE|    # OBSOLETE
#OBSOLETE|    def process_query_old(self, query, user):
#OBSOLETE|        """
#OBSOLETE|        \brief Compute the query plan related to a query which involves
#OBSOLETE|            no sub-queries. Sub-queries should already processed thanks to
#OBSOLETE|            process_subqueries().
#OBSOLETE|        \param query The query issued by the user.
#OBSOLETE|        \param user The user issuing the query (according to the user grants
#OBSOLETE|            the query plan might differ).
#OBSOLETE|        \return The AST instance representing the query plan.
#OBSOLETE|        """
#OBSOLETE|        # We process a single query without caring about 1..N
#OBSOLETE|        # former method
#OBSOLETE|        nodes = dict(self.g_3nf.graph.nodes(True)) # XXX
#OBSOLETE|
#OBSOLETE|        # Builds the query tree rooted at the fact table
#OBSOLETE|        root = self.g_3nf.find_node(query.get_from())
#OBSOLETE|        tree_edges = [e for e in self.g_3nf.get_tree_edges(root)] # generator
#OBSOLETE|
#OBSOLETE|        # Necessary fields are the one in the query augmented by the keys in the filters
#OBSOLETE|        needed_fields = set(query.fields)
#OBSOLETE|        if query.filters:
#OBSOLETE|            needed_fields.update(query.filters.keys())
#OBSOLETE|
#OBSOLETE|        # Prune the tree from useless tables
#OBSOLETE|        #visited_tree_edges = prune_query_tree(tree, tree_edges, nodes, needed_fields)
#OBSOLETE|        visited_tree_edges = DBGraph.prune_tree(tree_edges, nodes, needed_fields)
#OBSOLETE|        if not visited_tree_edges:
#OBSOLETE|            # The root table is sufficient to retrieve the queried fields
#OBSOLETE|            # OR WE COULD NOT ANSWER QUERY
#OBSOLETE|            q = Query(
#OBSOLETE|                action     = query.get_action(),
#OBSOLETE|                fact_table = root.name,
#OBSOLETE|                filters    = query.filters,
#OBSOLETE|                params     = query.params,
#OBSOLETE|                fields     = needed_fields,
#OBSOLETE|                ts         = query.ts
#OBSOLETE|            )
#OBSOLETE|            return AST(user).From(root, q) # root, needed_fields)
#OBSOLETE|
#OBSOLETE|        qp = None
#OBSOLETE|        root = True
#OBSOLETE|        for s, e in visited_tree_edges: # same order as if we would re-run a DFS
#OBSOLETE|            # We start at the root if necessary
#OBSOLETE|            if root:
#OBSOLETE|                # local_fields = fields required in the table we're considering
#OBSOLETE|                local_fields = set(needed_fields) & s.fields
#OBSOLETE|
#OBSOLETE|                it = iter(e.keys)
#OBSOLETE|                join_key = None
#OBSOLETE|                while join_key not in s.fields:
#OBSOLETE|                    join_key = it.next()
#OBSOLETE|                local_fields.add(join_key)
#OBSOLETE|
#OBSOLETE|                # We add fields necessary for performing joins = keys of all the children
#OBSOLETE|                # XXX does not work for multiple keys
#OBSOLETE|                ###print "LOCAL FIELDS", local_fields
#OBSOLETE|                ###for ss,ee in visited_tree_edges:
#OBSOLETE|                ###    if ss == s:
#OBSOLETE|                ###        local_fields.update(ee.keys)
#OBSOLETE|                ###print "LOCAL FIELDS", local_fields
#OBSOLETE|
#OBSOLETE|                if not local_fields:
#OBSOLETE|                    break
#OBSOLETE|
#OBSOLETE|                # We adopt a greedy strategy to get the required fields (temporary)
#OBSOLETE|                # We assume there are no partitions
#OBSOLETE|                first_join = True
#OBSOLETE|                left = AST(user)
#OBSOLETE|                sources = nodes[s]['sources'][:]
#OBSOLETE|                while True:
#OBSOLETE|                    max_table, max_fields = get_table_max_fields(local_fields, sources)
#OBSOLETE|                    if not max_table:
#OBSOLETE|                        raise Exception, 'get_table_max_fields error: could not answer fields: %r for query %s' % (local_fields, query)
#OBSOLETE|                    sources.remove(max_table)
#OBSOLETE|                    q = Query(action=query.get_action(), fact_table=max_table.get_name(), filters=query.filters, params=query.params, fields=list(max_fields))
#OBSOLETE|                    if first_join:
#OBSOLETE|                        left = AST(user).From(max_table, q) # max_table, list(max_fields))
#OBSOLETE|                        first_join = False
#OBSOLETE|                    else:
#OBSOLETE|                        right = AST(user).From(max_table, q) # max_table, list(max_fields))
#OBSOLETE|                        left = left.join(right, iter(s.keys).next())
#OBSOLETE|                    local_fields.difference_update(max_fields)
#OBSOLETE|                    needed_fields.difference_update(max_fields)
#OBSOLETE|                    if not local_fields:
#OBSOLETE|                        break
#OBSOLETE|                    # read the key
#OBSOLETE|                    local_fields.add(iter(s.keys).next())
#OBSOLETE|                qp = left
#OBSOLETE|                root = False
#OBSOLETE|
#OBSOLETE|            if not needed_fields:
#OBSOLETE|                return qp
#OBSOLETE|            local_fields = set(needed_fields) & e.fields
#OBSOLETE|
#OBSOLETE|            # Adding key for the join
#OBSOLETE|            it = iter(e.keys)
#OBSOLETE|            join_key = None
#OBSOLETE|            while join_key not in s.fields:
#OBSOLETE|                join_key = it.next()
#OBSOLETE|            local_fields.add(join_key)
#OBSOLETE|            # former ? local_fields.update(e.keys)
#OBSOLETE|
#OBSOLETE|            # We adopt a greedy strategy to get the required fields (temporary)
#OBSOLETE|            # We assume there are no partitions
#OBSOLETE|            first_join = True
#OBSOLETE|            left = AST(user)
#OBSOLETE|            sources = nodes[e]['sources'][:]
#OBSOLETE|            while True:
#OBSOLETE|                max_table, max_fields = get_table_max_fields(local_fields, sources)
#OBSOLETE|                if not max_table:
#OBSOLETE|                    break;
#OBSOLETE|                q = Query(action=query.get_action(), fact_table=max_table.get_name(), filters=query.filters, params=query.params, fields=list(max_fields))
#OBSOLETE|                if first_join:
#OBSOLETE|                    left = AST(user).From(max_table, q) # max_table, list(max_fields))
#OBSOLETE|                    first_join = False
#OBSOLETE|                else:
#OBSOLETE|                    right = AST(user).From(max_table, q) #max_table, list(max_fields))
#OBSOLETE|                    left = left.join(right, iter(e.keys).next())
#OBSOLETE|                local_fields.difference_update(max_fields)
#OBSOLETE|                needed_fields.difference_update(max_fields)
#OBSOLETE|                if not local_fields:
#OBSOLETE|                    break
#OBSOLETE|                # readd the key
#OBSOLETE|                local_fields.add(iter(e.keys).next())
#OBSOLETE|
#OBSOLETE|            key = iter(e.keys).next()
#OBSOLETE|            qp = qp.join(left, key) # XXX
#OBSOLETE|        return qp

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
            #except Exception, e:
            #    raise Exception, "Error during local request: %s" % e
        route = None

        #print "(forward)"

        # eg. a query arrive (similar to a packet)packet arrives (query)
        
        # we look at the destination of the query
        # valid destinations are the ones that form a DAG given the NF schema
        #destination = query.destination
        #print "(got destination)", destination
        #
        # In flow table ?
        #try:
        #    print "(searching for route in flow table)"
        #    route = self.flow_table[destination]
        #    print "(found route in flow table)"
        #except KeyError, key:
        #    print "(route not in flow table, try somewhere else)"
        #    # In FIB ?
        #    try:
        #        route = self.fib[destination]
        #    except KeyError, key:
        #        # In RIB ? raise exception if not found
        #        try:
        #            route = self.rib[destination]
        #        except KeyError, key:
        #            raise Exception, "Unknown destination: %r" % key
        #            return None
        #
        #        # Add to FIB
        #        fib[destination] = route
        #    
        #    # Add to flow table
        #    flow_table[destination] = route

        return self.do_forward(query, route, deferred, execute, user)
            
        # in tophat this is a AST + a set of queries to _next_hops_
        #  - we forward processed subqueries to next hops and we process them
        #  - out = f(FW(f(in, peer1)), FW(in, peer2), FW(...), ...)
        #    This is an AST !!! we need to decouple gateways for ends of the query plane / AST
        #  - a function of what to do with the list of results : not query by query but result by result... partial combination also work...
        #  - in fact a multipipe in which ot insert any result that come
        # in BGP this is a next hop to which to forward
        #
        # if the destination is not in the FIB, compute the associated route and add it, otherwise retrieve it (the most specific one)
        # TODO Steiner tree, dmst, spf, etc.
        # Typically a BGP router maintains a shortest path to all destinations, we don't do this.
        #
        # Eventually pass the message to the data plane, to establish query plane (route + operators from query) and circuits (gateways)
        #
        # Are we waiting for an answer or not (one shot query, callback (different communication channel), changes (risk of timeout), streaming)

    def do_forward(self, query, route, deferred, execute=True, user=None):
        """
        Effectively runs the forwarding of the query to the route
        """

        # the route parameter is ignored until we clearly state what are the
        # different entities of a router and their responsabilities



        if not execute: 
            self.get_query_plan(query, user)
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
        qp = self.get_query_plan(query, user)
        d = defer.Deferred() if deferred else None
        cb = Callback(d, router=self, cache_id=h)
        #cb = Callback(d, self.event, router=self, cache_id=h)
        qp.callback = cb

        # Now we only need to start it for Get.
        if query.get_action() == "get":
            pass

        elif query.get_action() == "update":
            # At the moment we can only update if the primary key is present
            keys = self.metadata_get_keys(query.get_from())
            if not keys:
                raise Exception, "Missing metadata for table %s" % query.get_from()
            key = list(keys).pop()
            
            if not query.filters.has_eq(key):
                raise Exception, "The key field '%s' must be present in update request" % key

        elif query.get_action() == "create":
           pass 

        else:
            raise Exception, "Action not supported: %s" % query.get_action()

        qp.start()

        if deferred: return d
        cb.wait()
        #self.event.wait()
        #self.event.clear()

        return cb.results
