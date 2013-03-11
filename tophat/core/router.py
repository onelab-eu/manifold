import os, sys, json, time, traceback, threading
from types                      import StringTypes

from twisted.internet           import defer

from tophat.router              import *
from tophat.core.filter         import Predicate
from tophat.core.ast            import AST
from tophat.core.key            import Key
from manifold.core.query        import Query
from tophat.core.table          import Table
from manifold.gateways          import *
from tophat.models              import *
from tophat.util.dbnorm         import DBNorm
from tophat.util.dbgraph        import DBGraph
from tophat.util.dfs            import dfs
from tophat.util.pruned_tree    import build_pruned_tree
from tophat.util.query_plane    import build_query_plane 
from tophat.util.reactor_thread import ReactorThread

from sfa.trust.credential       import Credential
from manifold.gateways.sfa        import ADMIN_USER
from tophat.metadata.Metadata   import import_file_h

METADATA_DIRECTORY = "/usr/share/myslice/metadata/"
CACHE_LIFETIME     = 1800

#------------------------------------------------------------------
# Class callback
#------------------------------------------------------------------

class Callback:
    def __init__(self, deferred=None, event=None, router=None, cache_id=None):
        self.results = []
        self._deferred = deferred
        self.event = event

        # Used for caching...
        self.router = router
        self.cache_id = cache_id

    def __call__(self, value):
        if not value:
            if self.cache_id:
                # Add query results to cache (expires in 30min)
                #print "Result added to cached under id", self.cache_id
                self.router.cache[self.cache_id] = (self.results, time.time() + CACHE_LIFETIME)

            if self._deferred:
                self._deferred.callback(self.results)
            else:
                self.event.set()
            return
        self.results.append(value)

#------------------------------------------------------------------
# Class THDestination
# Represent the destination (a query in our case) of a TopHat route
#------------------------------------------------------------------

class THDestination(Destination, Query):
    """
    Implements a destination in TopHat == a view == a query
    """
    
    def __str__(self):
        return "<THDestination / Query: %s" % self.query

#------------------------------------------------------------------
# Class THRoute
#------------------------------------------------------------------

class THRoute(Route):
    """
    Implements a TopHat route.
    """

    def __init__(self, destination, peer, cost, timestamp):

        if type(destination) != THDestination:
            raise TypeError("Destination of type %s expected in argument. Got %s" % (type(destination), THDestination))

        # Assert the route corresponds to an existing peer
        # Assert the cost corresponds to a right cost
        # Eventually the timestamp would not be a parameter but assigned

        super(THRoute, self).__init__(self, destination, peer, cost, timestamp)

    def push(identifier, record):
        pass

#------------------------------------------------------------------
# Class THCost
# Cost related to a route in the routing table
#------------------------------------------------------------------

class THCost(int):
    """
    Let's use (N, min, +) semiring for cost
    """

    def __add__(self, other):
        return THCost(min(self, other))

    def __mul__(self, other):
        return THCost(self + other)

#------------------------------------------------------------------
# Class THLocalRouter
# Router configured only with static/local routes, and which
# does not handle routing messages
#------------------------------------------------------------------

class THLocalRouter(LocalRouter):
    """
    Implements a TopHat router.

    Specialized to handle THAnnounces/THRoutes, ...
    """

    def __init__(self):
        print "I: Init THLocalRouter" 
        self.reactor = ReactorThread()
        LocalRouter.__init__(self, Table, object)
        self.event = threading.Event()
        # initialize dummy list of credentials to be uploaded during the
        # current session
        self.cache = {}

    def __enter__(self):
        self.reactor.startReactor()
        return self

    def __exit__(self, type, value, traceback):
        self.reactor.stopReactor()
        self.reactor.join()

    def import_file_h(self, directory, platform, gateway_type):
        """
        \brief Import a .h file (see tophat/metadata/*.h)
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
        try:
            gw = getattr(__import__('tophat.gateways', globals(), locals(), gtype), gtype)
        except Exception, e:
            raise Exception, "E: Cannot import gateway class '%s': %s" % (gtype, e)

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

    def build_tables(self):
        # Build one table per key {'Table' : THRoute}
        tables = self.rib.keys() # HUM
        self.G_nf = DBNorm(tables).g_3nf 

    def fetch_static_routes(self, directory = METADATA_DIRECTORY):
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
            #try:
            tables = self.import_file_h(
                directory,
                platform.platform,
                platform.gateway_type
            )
            #except Exception, why:
            #    print "ERROR in get_static_routes: ", why
            #    break
            
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

    def metadata_get_keys(self, table_name):
        for table in self.rib.keys(): # HUM
            if table.get_name() == table_name:
                return table.get_keys()
        return None

    def get_table(self, table_name):
        """
        \brief Retrieve the Table instance related to 'table_name' from the graph.
        \param table_name The name of the table
        \return The corresponding Table instance
        \sa tophat/core/table.py
        """
        for table in self.G_nf.graph.nodes(False):
            if table.name == table_name:
                return table

        raise ValueError("get_table: table not found (table_name = %s): available tables: %s" % (
            table_name,
            ["%r" % table for table in self.G_nf.graph.nodes(False)]
        ))

    def process_subqueries(self, query, user):
        """
        \brief Compute the AST (tree of SQL operators) related to a query
        \sa tophat/core/ast.py
        \param query The query issued by the user
        \param user The user
        \return An AST instance representing the query plane related to the query
        """
        print "=" * 100
        print "Entering process_subqueries %s (need fields %s) " % (query.fact_table, query.fields)
        print "=" * 100
        table_name = query.fact_table
        table = self.get_table(table_name)
        qp = AST(user)

        cur_filters = []
        cur_params = {}
        cur_fields = []
        subq = {}

        debug = "debug" in query.params and query.params["debug"]

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
                subts      = query.ts

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

                subquery = Query(query.action, method, subfilters, subparams, subfields, subts)

                child_ast = self.process_subqueries(subquery, user)
                children_ast.append(child_ast.root)

            parent = Query(query.action, query.fact_table, cur_filters, cur_params, cur_fields, query.ts)
            parent_ast = self.process_query(parent, user)
            qp = parent_ast
            qp.subquery(children_ast)
        else:
            parent = Query(query.action, query.fact_table, cur_filters, cur_params, cur_fields, query.ts)
            qp = self.process_query(parent, user)
            print "type(qp) = ", type(qp)

        return qp

    def get_table_max_fields(fields, tables):
        maxfields = 0
        ret = (None, None)
        for t in tables:
            isect = set(fields).intersection(t.fields)
            if len(isect) > maxfields:
                maxfields = len(isect)
                ret = (t, isect)
        return ret

    def get_query_plan(self, query, user):
        # DEBUG
        #print "get_query_plan: here is G_nf"
        #for node in dict(self.G_nf.graph.nodes(True)):
        #    print node

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

    def process_query(self, query, user):
#DEBUG|        print "Tables:"
#DEBUG|        for u in self.G_nf.graph.nodes(False):
#DEBUG|            print "%s" % u
        return self.process_query_mando(query, user)

    def process_query_mando(self, query, user):
        """
        \brief Compute the query plane related to a query which involves
            no sub-queries. Sub-queries should already processed thanks to
            process_subqueries().
        \param query The Query instance representing the query issued by the user.
            \sa manifold/core/query.py
        \param user The User instance reprensenting the user issuing
            the query. The query can be resolved in various way according to
            the user grants.
            \sa tophat/model/user.py
        \return The AST instance representing the query plane.
        """

        # Compute the fields involved explicitly in the query (e.g. in SELECT or WHERE)
        needed_fields = set(query.fields)
        if needed_fields == set():
            raise ValueError("No queried field")
        if query.filters:
            needed_fields.update(query.filters.keys())

        # Retrieve the root node corresponding to the fact table
        root = self.G_nf.get_root(query)

        # Retrieve the (unique due to 3-nf) tree included in "self.G_nf" and rooted in "root"
        # \sa tophat/util/dfs.py
        print "Entering DFS(%r) in graph:" % root
        for edge in self.G_nf.graph.edges():
            (u, v) = edge
            print "\t%r %s %r via %r" % (u, self.G_nf.graph[u][v]["type"], v, self.G_nf.graph[u][v]["info"])
        map_vertex_pred = dfs(self.G_nf.graph, root)

        # Compute the corresponding pruned tree.
        # Each node of the pruned tree only gathers relevant table, and only their
        # relevant fields and their relevant key (if used).
        # \sa tophat/util/pruned_graph.py
        pruned_tree = build_pruned_tree(self.G_nf.graph, needed_fields, map_vertex_pred)

        # Compute the skeleton resulting query plane
        # (e.g which does not take into account the query)
        # It leads to a query plane made of Union, From, and LeftJoin nodes
        return build_query_plane(user, pruned_tree)

#OBSOLETE|    def process_query_new(self, query, user):
#OBSOLETE|        """
#OBSOLETE|        \brief Compute the query plane related to a query which involves
#OBSOLETE|            no sub-queries. Sub-queries should already processed thanks to
#OBSOLETE|            process_subqueries().
#OBSOLETE|        \param query The query issued by the user.
#OBSOLETE|        \param user The user issuing the query (according to the user grants
#OBSOLETE|            the query plane might differ).
#OBSOLETE|        \return The AST instance representing the query plane.
#OBSOLETE|        """
#OBSOLETE|        # Find a tree of tables rooted at the fact table (e.g. method) included
#OBSOLETE|        # in the normalized graph.
#OBSOLETE|        root = self.G_nf.get_root(query)
#OBSOLETE|        tree = [arc for arc in self.G_nf.get_tree_edges(root)] # DFS tree 
#OBSOLETE|
#OBSOLETE|        # Compute the fields involved explicitly in the query (e.g. in SELECT or WHERE)
#OBSOLETE|        needed_fields = set(query.fields)
#OBSOLETE|        if query.filters:
#OBSOLETE|            needed_fields.update(query.filters.keys())
#OBSOLETE|
#OBSOLETE|        # Prune this tree to remove table that are not required to answer the query.
#OBSOLETE|        # As a consequence, each leave of the tree provides at least one queried field.
#OBSOLETE|        tree = DBGraph.prune_tree(tree, dict(self.G_nf.graph.nodes(True)), needed_fields)
#OBSOLETE|
#OBSOLETE|        # TODO check whether every needed_fields is provided by a node of the tree
#OBSOLETE|        # We might only be able to partially answer a query. Inform the user
#OBSOLETE|        # about it
#OBSOLETE|
#OBSOLETE|        # Initialize a query plan
#OBSOLETE|        qp = AST(user)
#OBSOLETE|
#OBSOLETE|        # Process the root node
#OBSOLETE|        successors = self.G_nf.get_successors(root)
#OBSOLETE|        print "W: Selecting an arbitrary key for each successors %r" % successors
#OBSOLETE|        succ_keys = [list(iter(table.get_keys()).next())[0] for table in successors]
#OBSOLETE|        current_fields = set(needed_fields) & set([field.get_name() for field in root.get_fields()])
#OBSOLETE|        current_fields |= set(succ_keys)
#OBSOLETE|        q = Query(
#OBSOLETE|            action     = query.action,
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
#OBSOLETE|        for _, node in self.G_nf.get_edges():
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
#OBSOLETE|            successors = self.G_nf.get_successors(node)
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
#OBSOLETE|                action     = query.action,
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
#OBSOLETE|        \brief Compute the query plane related to a query which involves
#OBSOLETE|            no sub-queries. Sub-queries should already processed thanks to
#OBSOLETE|            process_subqueries().
#OBSOLETE|        \param query The query issued by the user.
#OBSOLETE|        \param user The user issuing the query (according to the user grants
#OBSOLETE|            the query plane might differ).
#OBSOLETE|        \return The AST instance representing the query plane.
#OBSOLETE|        """
#OBSOLETE|        # We process a single query without caring about 1..N
#OBSOLETE|        # former method
#OBSOLETE|        nodes = dict(self.G_nf.graph.nodes(True)) # XXX
#OBSOLETE|
#OBSOLETE|        # Builds the query tree rooted at the fact table
#OBSOLETE|        root = self.G_nf.get_root(query)
#OBSOLETE|        tree_edges = [e for e in self.G_nf.get_tree_edges(root)] # generator
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
#OBSOLETE|                action     = query.action,
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
#OBSOLETE|                    q = Query(action=query.action, fact_table=max_table.name, filters=query.filters, params=query.params, fields=list(max_fields))
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
#OBSOLETE|                q = Query(action=query.action, fact_table=max_table.name, filters=query.filters, params=query.params, fields=list(max_fields))
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

    def do_forward(self, query, route, deferred, execute=True, user=None):
        """
        Effectively runs the forwarding of the query to the route
        """

        # the route parameter is ignored until we clearly state what are the
        # different entities of a router and their responsabilities



        if not execute: 
            self.get_query_plan(query, user)
            return None

        # The query plane will be the same whatever the action: it represents
        # the easier way to reach the destination = routing
        # We do not need the full query for the query plane, in fact just the
        # destination, which is a subpart of the query = (fact, filters, fields)
        # action = what to do on this QP
        # ts = how it behaves

        # Caching ?
        try:
            h = hash((user,query))
            #print "ID", h, ": looking into cache..."
        except:
            h = 0

        if query.action == 'get':
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
        cb = Callback(d, self.event, router=self, cache_id=h)
        qp.callback = cb

        # Now we only need to start it for Get.
        if query.action == 'get':
            pass

        elif query.action == 'update':
            # At the moment we can only update if the primary key is present
            keys = self.metadata_get_keys(query.fact_table)
            if not keys:
                raise Exception, "Missing metadata for table %s" % query.fact_table
            key = list(keys).pop()
            
            if not query.filters.has_eq(key):
                raise Exception, "The key field '%s' must be present in update request" % key

        elif query.action == 'create':
           pass 

        else:
            raise Exception, "Action not supported: %s" % query.action

        qp.start()

        if deferred: return d
        self.event.wait()
        self.event.clear()

        return cb.results

class THRouter(THLocalRouter, Router):
    pass


