import os, sys
import xml.etree.cElementTree as ElementTree

from copy import deepcopy
import traceback
import threading
from twisted.internet import defer

from tophat.util.xmldict import *
from tophat.util.reactor_thread import ReactorThread
from tophat.core.filter import Filter, Predicate
from tophat.core.param import Param
from tophat.router import *
from tophat.core.table import Table
# Can we get rid of the source manager ?
from tophat.core.sourcemgr import SourceManager
from tophat.gateways import *
from tophat.core.ast import AST
from tophat.core.query import Query
from tophat.models import *
from tophat.util.dbnorm import DBNorm
from tophat.util.dbgraph import DBGraph
import json
import time

from types import StringTypes
from sfa.trust.credential import Credential

XML_DIRECTORY = '/usr/share/myslice/metadata/'
CACHE_LIFETIME = 1800

class Callback:
    def __init__(self, deferred=None, event=None, router=None, cache_id=None):
        self.results = []
        self._deferred = deferred
        self.event=event

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

class THDestination(Destination, Query):
    """
    Implements a destination in TopHat == a query
    """
    
    def __str__(self):
        return "<THDestination / Query: %s" % self.query



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

class THCost(int):
    """
    Let's use (N, min,  +) semiring for cost
    """

    def __add__(self, other):
        return THCost(min(self, other))

    def __mul__(self, other):
        return THCost(self + other)

class THLocalRouter(LocalRouter):
    """
    Implements a TopHat router.

    Specialized to handle THAnnounces/THRoutes, ...
    """

    def __init__(self):
        print "I: Init THLocalRouter" 
        self.reactor = ReactorThread()
        self.sourcemgr = SourceManager(self.reactor)
        LocalRouter.__init__(self, Table, object)
        self.event = threading.Event()
        # initialize dummy list of credentials to be uploaded during the
        # current session
        self.cache = {}

    def __enter__(self):
        print "I: Starting THLocalRouter" 
        self.reactor.startReactor()
        return self

    def __exit__(self, type, value, traceback):
        self.reactor.stopReactor()
        print "I: Reactor thread stopped. Waiting for thread to terminate..."
        self.reactor.join()

    def import_file(self, directory, platform, gateway_type):
        f = os.path.join(directory, "%s.xml" % gateway_type)
        if not os.path.exists(f):
            f = os.path.join(directory, "%s-%s.xml" % (gateway_type, platform))
            if not os.path.exists(f):
                raise Exception, "Metadata file not found for platform='%s' and gateway_type='%s'" % (platform, gateway_type)

        routes = []
        print "I: Processing %s" % f 
        tree = ElementTree.parse(f)
        root = tree.getroot()
        md = XmlDictConfig(root)
        
        # Checking the presence of a platform section
        #if not 'platform' in md:
        #    raise Exception, "Error importing metadata file '%s': no platform specified" % metadata
        #p_dict = md['platform']
        #platform = p_dict['platform']

        # Checking the presence of a method section
        if not 'methods' in md:
            raise Exception, "Error importing metadata file '%s': no method section specified" % f 
        methods = md['methods']

        # Checking the presence of at least a method
        if not 'method' in methods:
            raise Exception, "Error importing metadata file '%s': no method specified" % f 
        methods = methods['method']

        if not isinstance(methods, list):
            methods = [methods]

        # Looping through the methods
        for method in methods:
            
            aliases = method['name'].split('|')

            #base = ['%s::%s' % (p_dict['platform'], aliases[0])]
            #base.extend(aliases)

            # XXX we currently restrict ourselves to the main alias 'nodes'
            tmp = [a for a in aliases if a == 'nodes']
            name = tmp[0] if tmp else aliases[0]

            # Checking the presence of a field section
            if not 'fields' in method:
                raise Exception, "Error importing metadata file '%s': no field section" % f 
            field_arr = method['fields']
            # Checking the presence of at least a field
            if not 'field' in field_arr:
                raise Exception, "Error importing metadata file '%s': no field specified" % f 

            # FIXME Currently we ignore detailed information about the fields
            if not isinstance(field_arr['field'], list):
                field_arr['field'] = [field_arr['field']]
            fields = [f['field'] for f in field_arr['field']]

            # Checking the presence of a keys section
            if not 'keys' in method:
                raise Exception, "Error importing metadata file  '%s': no key section" % f 
            key_arr = method['keys']
            # Checking the presence of at least a key
            if not 'key' in key_arr:
                raise Exception, "Error importing metadata file '%s': no key specified" % f 
            if not isinstance(key_arr['key'], list):
                keys = [key_arr['key']]
            else:
                keys = key_arr['key']
            
            # Creating a new Table for inserting into the RIB
            t = Table(platform, name, fields, keys)
            
            #print "Adding %s::%s to RIB" % (platform, name)
            self.rib[t] = platform
            routes.append(t)
        return routes

    def get_gateway(self, platform, query, user):
        # XXX Ideally, some parameters regarding MySlice user account should be
        # stored outside of the platform table

        # Finds the gateway corresponding to the platform
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

        # Get user account
        accounts = [a for a in user.accounts if a.platform.platform == platform]
        if not accounts:
            # No user account for platform, let's create it
            account = Account(user=user, platform=p, auth_type='managed', config='{}')
            db.add(account)
            db.commit()
        else:
            account = accounts[0]
        
        # Gateway config
        gconf = json.loads(p.config)

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

        #try:
        ret = gw(self, platform, query, gconf, aconf, user)
        #except Exception, e:
        #    raise Exception, "E: Cannot instantiate gateway for platform '%s': %s" % (platform, e)

        return ret

    def add_credential(self, cred, platform, user):
        print "I: Added credential of type", cred['type']

        account = [a for a in user.accounts if a.platform.platform == platform][0]

        config = account.config_get()

        if isinstance(cred, dict):
            cred = cred['cred']
        
        c = Credential(string=cred)
        c_type = c.get_gid_object().get_type()

        if c_type == 'user':
            config['user_credential'] = cred['cred']
        elif c_type == 'slice':
            if not 'slice_credentials' in config:
                config['slice_credentials'] = {}
            config['slice_credentials'][cred['target']] = cred['cred']
        elif c_type == 'authority':
            config['authority_credential'] = cred['cred']
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
        #    'credential_person_id': self.caller['person_id'],
        #    'credential_target': cred.get_gid_object().get_hrn(),
        #    'credential_expiration':  cred.get_expiration(),
        #    'credential_type': cred.get_gid_object().get_type(),
        #    'credential': cred.save_to_string()
        #}

    def cred_to_struct(self, cred):
        c = Credential(string=cred)
        return {
            'target': c.get_gid_object().get_hrn(),
            'expiration':  c.get_expiration(),
            'type': c.get_gid_object().get_type(),
            'credential': c.save_to_string()
        }

    def get_credentials(self, platform, user):
        creds = []

        account = [a for a in user.accounts if a.platform.platform == platform][0]
        config = account.config_get()

        creds.append(self.cred_to_struct(config['user_credential']))
        for sc in config['slice_credentials'].values():
            creds.append(self.cred_to_struct(sc))
        creds.append(self.cred_to_struct(config['authority_credential']))

        return creds

    def build_tables(self):
        tables = self.rib.keys() # HUM
        # Table normalization
        tables_3nf = DBNorm(tables).tables_3nf
        # Join graph
        self.G_nf = DBGraph(tables_3nf)

    def get_static_routes(self, directory):
        routes = []
        # Instead of iterating through files, we are now iterating thorough the
        # set of available platforms in the database
        platforms = db.query(Platform).filter(Platform.disabled == False).all()
        for p in platforms:
            gateway = None
            tables = self.import_file(XML_DIRECTORY, p.platform, p.gateway_type)
            routes.extend(tables)
            # NOT USED: tables

        #for root, dirs, files in os.walk(directory):
        #    for d in dirs[:]:
        #        if d[0] == '.':
        #            dirs.remove(d)
        #    metadata = [f for f in files if f[-3:] == 'xml']
        #    for m in metadata:
        #        # This builds the RIB in fact
        #        route_arr = self.import_file(os.path.join(root, m))
        #        routes.extend(route_arr)
        return routes

    def get_platform_max_fields(self, fields, join):
        # Search for the platform::method that allows for the largest number of missing fields
        _fields = [f.split('.')[0] for f in fields]
        maxfields = 0
        ret = (None, None)
        
        for dest, route in self.rib.items():
            # HACK to make tophat on join
            if not join and dest.platform in ['tophat', 'myslice']:
                continue
            isect = set(dest.fields).intersection(set(_fields))
            if len(isect) > maxfields:
                maxfields = len(isect)
                ret = (dest, isect)
        return ret

    def metadata_get_keys(self, table_name):
        for t in self.rib.keys(): # HUM
            if t.name == table_name:
                return t.keys
        return None



    def do_forward(self, query, route, deferred, execute=True, user=None):
        """
        Effectively runs the forwarding of the query to the route
        """

        # the route parameter is ignored until we clearly state what are the
        # different entities of a router and their responsabilities

        def process_query(query, user):
            # We process a single query without caring about 1..N
            # former method
            nodes = dict(self.G_nf.graph.nodes(True)) # XXX

            # Builds the query tree rooted at the fact table
            root = self.G_nf.get_root(query)
            tree_edges = [e for e in self.G_nf.get_tree_edges(root)] # generator

            # Necessary fields are the one in the query augmented by the keys in the filters
            needed_fields = set(query.fields)
            if query.filters:
                needed_fields.update(query.filters.keys())

            # Prune the tree from useless tables
            #visited_tree_edges = prune_query_tree(tree, tree_edges, nodes, needed_fields)
            visited_tree_edges = DBGraph.prune_tree(tree_edges, nodes, needed_fields)
            if not visited_tree_edges:
                # The root is sufficient
                # OR WE COULD NOT ANSWER QUERY
                q = Query(action=query.action, fact_table=root.name, filters=query.filters, params=query.params, fields=needed_fields)
                return AST(self, user).From(root, q) # root, needed_fields)

            qp = None
            root = True
            for s, e in visited_tree_edges:
                # We start at the root if necessary
                if root:
                    local_fields = set(needed_fields) & s.fields

                    it = iter(e.keys)
                    join_key = None
                    while join_key not in s.fields:
                        join_key = it.next()
                    local_fields.add(join_key)

                    # We add fields necessary for performing joins = keys of all the children
                    # XXX does not work for multiple keys
                    ###print "LOCAL FIELDS", local_fields
                    ###for ss,ee in visited_tree_edges:
                    ###    if ss == s:
                    ###        local_fields.update(ee.keys)
                    ###print "LOCAL FIELDS", local_fields

                    if not local_fields:
                        break

                    # We adopt a greedy strategy to get the required fields (temporary)
                    # We assume there are no partitions
                    first_join = True
                    left = AST(self, user)
                    sources = nodes[s]['sources'][:]
                    while True:
                        max_table, max_fields = get_table_max_fields(local_fields, sources)
                        if not max_table:
                            raise Exception, 'get_table_max_fields error: could not answer fields: %r for query %s' % (local_fields, query)
                        sources.remove(max_table)
                        q = Query(action=query.action, fact_table=max_table.name, filters=query.filters, params=query.params, fields=list(max_fields))
                        if first_join:
                            left = AST(self, user).From(max_table, q) # max_table, list(max_fields))
                            first_join = False
                        else:
                            right = AST(self, user).From(max_table, q) # max_table, list(max_fields))
                            left = left.join(right, iter(s.keys).next())
                        local_fields.difference_update(max_fields)
                        needed_fields.difference_update(max_fields)
                        if not local_fields:
                            break
                        # read the key
                        local_fields.add(iter(s.keys).next())
                    qp = left
                    root = False

                if not needed_fields:
                    return qp
                local_fields = set(needed_fields) & e.fields

                # Adding key for the join
                it = iter(e.keys)
                join_key = None
                while join_key not in s.fields:
                    join_key = it.next()
                local_fields.add(join_key)
                # former ? local_fields.update(e.keys)

                # We adopt a greedy strategy to get the required fields (temporary)
                # We assume there are no partitions
                first_join = True
                left = AST(self, user)
                sources = nodes[e]['sources'][:]
                while True:
                    max_table, max_fields = get_table_max_fields(local_fields, sources)
                    if not max_table:
                        break;
                    q = Query(action=query.action, fact_table=max_table.name, filters=query.filters, params=query.params, fields=list(max_fields))
                    if first_join:
                        left = AST(self, user).From(max_table, q) # max_table, list(max_fields))
                        first_join = False
                    else:
                        right = AST(self, user).From(max_table, q) #max_table, list(max_fields))
                        left = left.join(right, iter(e.keys).next())
                    local_fields.difference_update(max_fields)
                    needed_fields.difference_update(max_fields)
                    if not local_fields:
                        break
                    # readd the key
                    local_fields.add(iter(e.keys).next())

                key = iter(e.keys).next()
                qp = qp.join(left, key) # XXX
            return qp
            

        def process_subqueries(query, user):
            qp = AST(self, user)

            cur_filters = []
            cur_params = {}
            cur_fields = []
            subq = {}

            # XXX there are some parameters that will be answered by the parent !!!! no need to request them from the children !!!!
            # XXX XXX XXX XXX XXX XXX ex slice.resource.PROPERTY

            if query.filters:
                for pred in query.filters:
                    if '.' in pred.key:
                        method, subkey = pred.key.split('.', 1)
                        if not method in subq:
                            subq[method] = {}
                        if not 'filters' in subq[method]:
                            subq[method]['filters'] = []
                        subq[method]['filters'].append(Predicate(subkey, pred.op, pred.value))
                    else:
                        cur_filters.append(pred)

            if query.params:
                for key, value in query.params.items():
                    if '.' in key:
                        method, subkey = key.split('.', 1)
                        if not method in subq:
                            subq[method] = {}
                        if not 'params' in subq[method]:
                            subq[method]['params'] = {}
                        subq[method]['params'][subkey, value]
                    else:
                        cur_params[key] = value

            if query.fields:
                for field in query.fields:
                    if '.' in field:
                        method, subfield = field.split('.', 1)
                        if not method in subq:
                            subq[method] = {}
                        if not 'fields' in subq[method]:
                            subq[method]['fields'] = []
                        subq[method]['fields'].append(subfield)
                    else:
                        cur_fields.append(field)

            if len(subq):
                children_ast = []
                for method, subquery in subq.items():
                    # We need to add the keys of each subquery
                    # 
                    # We append the method name (eg. resources) which should return the list of keys
                    # (and eventually more information, but they will be ignored for the moment)
                    if not method in cur_fields:
                        cur_fields.append(method)

                    # Recursive construction of the processed subquery
                    subfilters = subquery['filters'] if 'filters' in subquery else []
                    subparams = subquery['params'] if 'params' in subquery else []
                    subfields = subquery['fields'] if 'fields' in subquery else []

                    # XXX Adding primary key in subquery to be able to merge
                    keys = self.metadata_get_keys(method)
                    if not keys:
                        raise Exception, "Cannot build children query: method %s has no key" % method
                    key = list(keys).pop()
                    subfields.append(key)

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
                    subquery = Query(query.action, method, subfilters, subparams, subfields)

                    child_ast = process_subqueries(subquery, user)
                    children_ast.append(child_ast.root)

                parent = Query(query.action, query.fact_table, cur_filters, cur_params, cur_fields)
                parent_ast = process_query(parent, user)
                qp = parent_ast
                qp.subquery(children_ast)
            else:
                parent = Query(query.action, query.fact_table, cur_filters, cur_params, cur_fields)
                qp = process_query(parent, user)
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

        def get_query_plan(query, user):
            try:
                qp = process_subqueries(query, user)
            except Exception ,e:
                print "Exception in do_forward", e
                traceback.print_exc()
                return []

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

        if not execute: 
            get_query_plan(query, user)
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
        qp = get_query_plan(query, user)
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


