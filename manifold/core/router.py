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

#DEPRECATED#    def get_gateway(self, platform, query, user):
#DEPRECATED#        # XXX Ideally, some parameters regarding MySlice user account should be
#DEPRECATED#        # stored outside of the platform table
#DEPRECATED#
#DEPRECATED#        # Finds the gateway corresponding to the platform
#DEPRECATED#        if isinstance(platform, (tuple, list, set, frozenset)):
#DEPRECATED#            if len(list(platform)) > 1:
#DEPRECATED#                print "W: get_gateway: only keeping the first platform in %s" % platform
#DEPRECATED#            platform = list(platform)[0]
#DEPRECATED#
#DEPRECATED#        try:
#DEPRECATED#            p = db.query(Platform).filter(Platform.platform == platform).one()
#DEPRECATED#        except Exception, e:
#DEPRECATED#            raise Exception, "E: Missing gateway information for platform '%s': %s" % (platform, e)
#DEPRECATED#
#DEPRECATED#        # Get the corresponding class
#DEPRECATED#        gtype = p.gateway_type.encode('latin1')
#DEPRECATED#        gw = Gateway.get(gtype)
#DEPRECATED#        #try:
#DEPRECATED#        #    gw = getattr(__import__('tophat.gateways', globals(), locals(), gtype), gtype)
#DEPRECATED#        #except Exception, e:
#DEPRECATED#        #    raise Exception, "E: Cannot import gateway class '%s': %s" % (gtype, e)
#DEPRECATED#
#DEPRECATED#        # Gateway config
#DEPRECATED#        gconf = json.loads(p.config)
#DEPRECATED#
#DEPRECATED#        # Get user account
#DEPRECATED#        accounts = [a for a in user.accounts if a.platform.platform == platform]
#DEPRECATED#        if not accounts:
#DEPRECATED#            # No user account for platform, let's skip it
#DEPRECATED#            print "E: No user account for platform : %s" % platform
#DEPRECATED#            account = None 
#DEPRECATED#            return gw(self, platform, query, gconf, None, user)
#DEPRECATED#            #account = Account(user=user, platform=p, auth_type='managed', config='{}')
#DEPRECATED#            #db.add(account)
#DEPRECATED#            #db.commit()
#DEPRECATED#        else:
#DEPRECATED#            account = accounts[0]
#DEPRECATED#        
#DEPRECATED#
#DEPRECATED#        # User account config
#DEPRECATED#        if account.auth_type == 'reference':
#DEPRECATED#            ref_platform = json.loads(account.config)['reference_platform']
#DEPRECATED#            ref_accounts = [a for a in user.accounts if a.platform.platform == ref_platform]
#DEPRECATED#            if not ref_accounts:
#DEPRECATED#                raise Exception, "reference account does not exist"
#DEPRECATED#            ref_account = ref_accounts[0]
#DEPRECATED#            aconf = ref_account.config
#DEPRECATED#        else:
#DEPRECATED#            aconf = account.config
#DEPRECATED#        aconf = json.loads(aconf)
#DEPRECATED#
#DEPRECATED##        if account.auth_type == 'reference':
#DEPRECATED##            reference_platform = json.loads(account.config)['reference_platform']
#DEPRECATED##            ref_accounts = [a for a in user.accounts if a.platform.platform == reference_platform]
#DEPRECATED##            if not ref_accounts:
#DEPRECATED##                raise Exception, "reference account does not exist"
#DEPRECATED##            ref_account = ref_accounts[0]
#DEPRECATED##            aconf = json.loads(ref_account.config) if account else None
#DEPRECATED##            if aconf: aconf['reference_platform'] = reference_platform
#DEPRECATED##        else:
#DEPRECATED##            aconf = json.loads(account.config) if account else None
#DEPRECATED#
#DEPRECATED#        return gw(self, platform, query, gconf, aconf, user)

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

# DEPRECATED     @returns(list)
# DEPRECATED     def metadata_get_tables(self):
# DEPRECATED         """
# DEPRECATED         \return The list of Table instances announced in the metadata
# DEPRECATED             collected by this router
# DEPRECATED         """
# DEPRECATED         return self.rib.keys() # HUM

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

# DEPRECATED     def fetch_static_routes(self, directory = STATIC_ROUTES_FILE):
# DEPRECATED         """
# DEPRECATED         \brief Retrieve static routes related to each plaform. 
# DEPRECATED             See:
# DEPRECATED                 sqlite3 /var/myslice/db.sqlite
# DEPRECATED                 > select platform, gateway_type from platform;
# DEPRECATED         \param directory Containing static route files. 
# DEPRECATED         \return The corresponding static routes
# DEPRECATED         """
# DEPRECATED         routes = []
# DEPRECATED 
# DEPRECATED         # Query the database to retrieve which configuration file has to be
# DEPRECATED         # loaded for each platform. 
# DEPRECATED         platforms = db.query(Platform).filter(Platform.disabled == False).all()
# DEPRECATED 
# DEPRECATED         # For each platform, load the corresponding .h file
# DEPRECATED         for platform in platforms:
# DEPRECATED             gateway = None
# DEPRECATED             try:
# DEPRECATED                 tables = self.import_file_h(
# DEPRECATED                     directory,
# DEPRECATED                     platform.platform,
# DEPRECATED                     platform.gateway_type
# DEPRECATED                 )
# DEPRECATED             except Exception, why:
# DEPRECATED                 print "Error while importing %s in get_static_routes: %s" % (platform, why)
# DEPRECATED             
# DEPRECATED             routes.extend(tables)
# DEPRECATED 
# DEPRECATED         return routes

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

#DEPRECATED#    def process_subqueries(self, query, user):
#DEPRECATED#        """
#DEPRECATED#        \brief Compute the AST (tree of SQL operators) related to a query
#DEPRECATED#        \sa manifold.core.ast.py
#DEPRECATED#        \param query A Query issued by the user
#DEPRECATED#        \param user A User instance (carry user's information) 
#DEPRECATED#        \return An AST instance representing the query plan related to the query
#DEPRECATED#        """
#DEPRECATED#        print "=" * 100
#DEPRECATED#        print "Entering process_subqueries %s (need fields %s) " % (query.get_from(), query.get_select())
#DEPRECATED#        print "=" * 100
#DEPRECATED#        table_name = query.get_from()
#DEPRECATED#        table = self.g_3nf.find_node(table_name)
#DEPRECATED#        if not table:
#DEPRECATED#            raise ValueError("Can't find table %r related to query %r" % (table_name, query))
#DEPRECATED#
#DEPRECATED#        qp = AST(user)
#DEPRECATED#
#DEPRECATED#        analyzed_query = AnalyzedQuery(query)
#DEPRECATED#        for subquery in analyzed_query.subqueries():
#DEPRECATED#
#DEPRECATED#            method = table.get_field(method).get_type()
#DEPRECATED#            if not method in cur_fields:
#DEPRECATED#                subquery.select(method)
#DEPRECATED#
#DEPRECATED#            # Adding primary key in subquery to be able to merge
#DEPRECATED#            keys = self.metadata_get_keys(method)
#DEPRECATED#            if keys:
#DEPRECATED#                key = list(keys).pop()
#DEPRECATED#                print "W: selecting arbitrary key %s to join with '%s'" % (key, method)
#DEPRECATED#                if isinstance(key, Key):
#DEPRECATED#                    for field in key:
#DEPRECATED#                        field_name = field.get_name()
#DEPRECATED#                        if field_name not in subfields:
#DEPRECATED#                            subquery.select(field_name)
#DEPRECATED#                else:
#DEPRECATED#                    raise TypeError("Invalid type: key = %s (type %s)" % (key, type(key)))
#DEPRECATED#
#DEPRECATED#            child_ast = self.process_subqueries(subquery, user)
#DEPRECATED#            children_ast.append(child_ast.root)
#DEPRECATED#
#DEPRECATED#            parent_ast = self.process_query(analyzed_query, user)
#DEPRECATED#            qp = parent_ast
#DEPRECATED#            qp.subquery(children_ast)
#DEPRECATED#        else:
#DEPRECATED#            qp = self.process_query(analyzed_query, user)
#DEPRECATED#
#DEPRECATED#        return qp

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



#OBSOLETE|    def get_table_max_fields(fields, tables):
#OBSOLETE|        maxfields = 0
#OBSOLETE|        ret = (None, None)
#OBSOLETE|        for t in tables:
#OBSOLETE|            isect = set(fields).intersection(t.fields)
#OBSOLETE|            if len(isect) > maxfields:
#OBSOLETE|                maxfields = len(isect)
#OBSOLETE|                ret = (t, isect)
#OBSOLETE|        return ret

#DEPRECATED#    def get_query_plan(self, query, user):
#DEPRECATED#        """
#DEPRECATED#        \brief Compute the query plan related to a user's query
#DEPRECATED#        \param query The Query issued by the user
#DEPRECATED#        \param user A User instance
#DEPRECATED#        \return An AST instance
#DEPRECATED#        """
#DEPRECATED#        qp = self.process_subqueries(query, user)
#DEPRECATED#
#DEPRECATED#        # Now we apply the operators
#DEPRECATED#        #qp = qp.selection(query.filters) 
#DEPRECATED#        #qp = qp.projection(query.fields) 
#DEPRECATED#        #qp = qp.sort(query.get_sort()) 
#DEPRECATED#        #qp = qp.limit(query.get_limit()) 
#DEPRECATED#
#DEPRECATED#        # We should now have a query plan
#DEPRECATED#        print ""
#DEPRECATED#        print "QUERY PLAN:"
#DEPRECATED#        print "-----------"
#DEPRECATED#        qp.dump()
#DEPRECATED#        print ""
#DEPRECATED#        print ""
#DEPRECATED#
#DEPRECATED#        return qp

#DEPRECATED#    #---------------
#DEPRECATED#    # Local queries
#DEPRECATED#    #---------------
#DEPRECATED#
#DEPRECATED#    def local_query_get(self, query):
#DEPRECATED#        #
#DEPRECATED#        # XXX How are we handling subqueries
#DEPRECATED#        #
#DEPRECATED#
#DEPRECATED#        fields = query.fields
#DEPRECATED#        # XXX else tap into metadata
#DEPRECATED#
#DEPRECATED#        cls = self._map_local_table[query.fact_table]
#DEPRECATED#
#DEPRECATED#        # Transform a Filter into a sqlalchemy expression
#DEPRECATED#        _filters = get_sqla_filters(cls, query.filters)
#DEPRECATED#        _fields = xgetattr(cls, query.fields) if query.fields else None
#DEPRECATED#
#DEPRECATED#        if query.fields:
#DEPRECATED#            res = db.query( *_fields ).filter(_filters)
#DEPRECATED#        else:
#DEPRECATED#            res = db.query( cls ).filter(_filters)
#DEPRECATED#
#DEPRECATED#        tuplelist = res.all()
#DEPRECATED#        # only 2.7+ table = [ { fields[idx] : val for idx, val in enumerate(t) } for t in tuplelist]
#DEPRECATED#        table = [ dict([(fields[idx], val) for idx, val in enumerate(t)]) for t in tuplelist]
#DEPRECATED#        return table
#DEPRECATED#
#DEPRECATED#    def local_query_update(self, query):
#DEPRECATED#
#DEPRECATED#        cls = self._map_local_table[query.fact_table]
#DEPRECATED#
#DEPRECATED#        _fields = xgetattr(cls, query.fields)
#DEPRECATED#        _filters = get_sqla_filters(cls, query.filters)
#DEPRECATED#        # only 2.7+ _params = { getattr(cls, k): v for k,v in query.params.items() }
#DEPRECATED#        _params = dict([ (getattr(cls, k), v) for k,v in query.params.items() ])
#DEPRECATED#
#DEPRECATED#        #db.query(cls).update(_params, synchronize_session=False)
#DEPRECATED#        db.query(cls).filter(_filters).update(_params, synchronize_session=False)
#DEPRECATED#        db.commit()
#DEPRECATED#
#DEPRECATED#        return []
#DEPRECATED#
#DEPRECATED#    def local_query_create(self, query):
#DEPRECATED#
#DEPRECATED#        assert not query.filters, "Filters should be empty for a create request"
#DEPRECATED#        #assert not query.fields, "Fields should be empty for a create request"
#DEPRECATED#
#DEPRECATED#
#DEPRECATED#        cls = self._map_local_table[query.fact_table]
#DEPRECATED#        params = cls.process_params(query.params)
#DEPRECATED#        new_obj = cls(**params)
#DEPRECATED#        db.add(new_obj)
#DEPRECATED#        db.commit()
#DEPRECATED#        
#DEPRECATED#        return []
#DEPRECATED#
#DEPRECATED#    def local_query(self, query):
#DEPRECATED#        _map_action = {
#DEPRECATED#            "get"    : self.local_query_get,
#DEPRECATED#            "update" : self.local_query_update,
#DEPRECATED#            "create" : self.local_query_create
#DEPRECATED#        }
#DEPRECATED#        return _map_action[query.action](query)

#DEPRECATED#    @returns(AST)
#DEPRECATED#    def process_query(self, query, user):
#DEPRECATED#        return self.process_query_mando(query, user)
#DEPRECATED#
#DEPRECATED#    @returns(AST)
#DEPRECATED#    def process_query_mando(self, query, user):
#DEPRECATED#        """
#DEPRECATED#        \brief Compute the query plan related to a query which involves
#DEPRECATED#            no sub-queries. Sub-queries should already processed thanks to
#DEPRECATED#            process_subqueries().
#DEPRECATED#        \param query The Query instance representing the query issued by the user.
#DEPRECATED#            \sa manifold/core/query.py
#DEPRECATED#        \param user The User instance reprensenting the user issuing
#DEPRECATED#            the query. The query can be resolved in various way according to
#DEPRECATED#            the user grants.
#DEPRECATED#            \sa tophat/model/user.py
#DEPRECATED#        \return The AST instance representing the query plan.
#DEPRECATED#        """
#DEPRECATED#
#DEPRECATED#        # Compute the fields involved explicitly in the query (e.g. in SELECT or WHERE)
#DEPRECATED#        needed_fields = set(query.get_select())
#DEPRECATED#        if needed_fields == set():
#DEPRECATED#            raise ValueError("No queried field")
#DEPRECATED#        needed_fields.update(query.get_where().keys())
#DEPRECATED#
#DEPRECATED#        # Retrieve the root node corresponding to the fact table
#DEPRECATED#        root = self.g_3nf.find_node(query.get_from())
#DEPRECATED#
#DEPRECATED#        # Retrieve the (unique due to 3-nf) tree included in "self.g_3nf" and rooted in "root"
#DEPRECATED#        # \sa manifold.util.dfs.py
#DEPRECATED#        print "Entering DFS(%r) in graph:" % root
#DEPRECATED#
#DEPRECATED#        # Compute the corresponding pruned tree.
#DEPRECATED#        # Each node of the pruned tree only gathers relevant table, and only their
#DEPRECATED#        # relevant fields and their relevant key (if used).
#DEPRECATED#        # \sa manifold.util.pruned_graph.py
#DEPRECATED#        pruned_tree = build_pruned_tree(self.g_3nf.graph, needed_fields, dfs(self.g_3nf.graph, root))
#DEPRECATED#
#DEPRECATED#        # Compute the skeleton resulting query plan
#DEPRECATED#        # (e.g which does not take into account the query)
#DEPRECATED#        # It leads to a query plan made of Union, From, and LeftJoin nodes
#DEPRECATED#        return build_query_plan(user, query, pruned_tree)

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
