import os, sys
import xml.etree.cElementTree as ElementTree

from copy import deepcopy
import traceback
import threading
from twisted.internet import defer

import networkx as nx
import matplotlib.pyplot as plt
from networkx.algorithms.traversal.depth_first_search import dfs_tree, dfs_edges

from tophat.util.xmldict import *
from tophat.util.reactor_thread import ReactorThread
from tophat.core.filter import Filter, Predicate
from tophat.core.param import Param
from tophat.router import *
from tophat.core.sourcemgr import SourceManager
from tophat.gateways import *
from tophat.core.ast import AST
from tophat.core.query import Query
from tophat.models import *
import json


#from tophat.models import session, Platform


UNIT_COST = 1

class Callback:
    def __init__(self, deferred=None, event=None):
        self.results = []
        self._deferred = deferred
        self.event=event

    def __call__(self, value):
        if not value:
            if self._deferred:
                self._deferred.callback(self.results)
            else:
                self.event.set()
            return
        # XXX What if we have multiple queries in parallel ?
        # we need to stored everything in separated lists
        self.results.append(value)

class Table:
    """
    Implements a database table schema.
    """

    def __init__(self, platform, name, fields, keys, partition=None, cost=1):
        self.platform = platform
        self.name = name
        self.fields = fields
        self.keys = keys
        self.partition = partition # an instance of a Filter
        # There will also be a list that the platform cannot provide, cf sources[i].fields
        self.cost = cost
        if isinstance(self.keys, (list, tuple)):
            self.keys = frozenset(self.keys)
        if isinstance(self.fields, (list, tuple)):
            self.fields = frozenset(self.fields)

    def __str__(self):
        #return "<Table name='%s' platform='%s' fields='%r' keys='%r'>" % (self.name, self.platform, self.fields, self.keys)
        if self.platform:
            return "%s::%s" % (self.platform, self.name)
        else:
            return self.name

    def get_fields_from_keys(self):
        fields = []
        for k in self.keys:
            if isinstance(k, (list, tuple)):
                fields.extend(list(k))
            else:
                fields.append(k)
        return fields



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
        self.reactor = ReactorThread()
        self.sourcemgr = SourceManager(self.reactor)
        LocalRouter.__init__(self, Table, object)
        self.event = threading.Event()
        # initialize dummy list of credentials to be uploaded during the
        # current session
        self.creds = []

    def __enter__(self):
        self.reactor.startReactor()
        return self

    def __exit__(self, type, value, traceback):
        self.reactor.stopReactor()
        print "I: Reactor thread stopped. Waiting for thread to terminate..."
        self.reactor.join()

    def import_file(self, metadata):
        routes = []
        #print "I: Processing %s" % metadata
        tree = ElementTree.parse(metadata)
        root = tree.getroot()
        md = XmlDictConfig(root)
        
        # Checking the presence of a platform section
        if not 'platform' in md:
            raise Exception, "Error importing metadata file '%s': no platform specified" % metadata
        p_dict = md['platform']
        platform = p_dict['platform']

        # Update the peer/session table with platform and associated configuration
        # XXX
#        # Let's store gateway related information into the database too
#        gateways = MetadataGateways(self.api, {'platform': p['platform']})
#
#        # We add the platform name to the configuration
#        config = md['gateway']
#        config['platform'] = p['platform']
#
#        if not gateways:
#            print "I: Inserting new gateway for platform '%(platform)s'..." % p
#            g_dict = {
#                'config': json.dumps(config),
#                'platform_id': p['platform_id']
#            }
#            g = MetadataGateway(self.api, g_dict)
#            g.sync()
#        else:
#            print "I: Existing gateway for platform  '%(platform)s'. Updating..." % p
#            g = gateways[0]
#            g['config'] = json.dumps(config)
#            g.sync()

        # Checking the presence of a method section
        if not 'methods' in md:
            raise Exception, "Error importing metadata file '%s': no method section specified" % metadata
        methods = md['methods']

        # Checking the presence of at least a method
        if not 'method' in methods:
            raise Exception, "Error importing metadata file '%s': no method specified" % metadata
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
                raise Exception, "Error importing metadata file '%s': no field section" % metadata
            field_arr = method['fields']
            # Checking the presence of at least a field
            if not 'field' in field_arr:
                raise Exception, "Error importing metadata file '%s': no field specified" % metadata

            # FIXME Currently we ignore detailed information about the fields
            if not isinstance(field_arr['field'], list):
                field_arr['field'] = [field_arr['field']]
            fields = [f['field'] for f in field_arr['field']]

            # Checking the presence of a keys section
            if not 'keys' in method:
                raise Exception, "Error importing metadata file  '%s': no key section" % metadata
            key_arr = method['keys']
            # Checking the presence of at least a key
            if not 'key' in key_arr:
                raise Exception, "Error importing metadata file '%s': no key specified" % metadata
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
            p = session.query(Platform).filter(Platform.platform == platform).one()
        except Exception, e:
            raise Exception, "E: Missing gateway information for platform '%s': %s" % (platform, e)

        # Get the corresponding class
        gtype = p.gateway_type.encode('latin1')
        try:
            gw = getattr(__import__('tophat.gateways', globals(), locals(), gtype), gtype)
        except Exception, e:
            raise Exception, "E: Cannot import gateway class '%s': %s" % (gtype, e)

        # Get user account
        try:
            account = [a for a in user.accounts if a.platform.platform == platform][0]
        except Exception, e:
            account = None
            print "E: No user account found for platform '%s': %s" % (platform, e)
        
        gconf = json.loads(p.gateway_conf)
        aconf = json.loads(account.config) if account else None

        try:
            ret = gw(self, platform, query, gconf, aconf, user)
        except Exception, e:
            raise Exception, "E: Cannot instantiate gateway for platform '%s': %s" % (platform, e)

        return ret

    def add_credential(self, cred, platform, user):
        print "I: Added credential of type", cred['type']

        account = [a for a in user.accounts if a.platform.platform == platform][0]

        config = account.config_get()
        if cred['type'] == 'user':
            config['user_credential'] = cred['cred']
        elif cred['type'] == 'slice':
            if not 'slice_credentials' in config:
                config['slice_credentials'] = {}
            config['slice_credentials'][cred['target']] = cred['cred']
        else:
            raise Exception, "Invalid credential type"
        account.config_set(config)

    def get_static_routes(self, directory):
        routes = []
        for root, dirs, files in os.walk(directory):
            for d in dirs[:]:
                if d[0] == '.':
                    dirs.remove(d)
            metadata = [f for f in files if f[-3:] == 'xml']
            for m in metadata:
                route_arr = self.import_file(os.path.join(root, m))
                routes.extend(route_arr)
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

#    def compute_query_plan(self, query):
#
#        # XXX this should be replaced by a Steiner Tree computation
#        # XXX This should manage subqueries !!!!
#
#        from tophat.core.ast import AST
#        from tophat.core.metadata import Metadata
#        from tophat.core.gateway import Gateway
#
#        fact_table, filters, fields = query.fact_table, query.filters, query.fields
#
#        # This method is broken, need to replace it with steiner
#
#        if filters:
#            fields.extend(filters.keys())
#        fields = set(fields)
#         
#        # Query plan 
#        qp = AST(self)
#        join = False 
#
#        # Note: We could skip or restrict the set of platforms, and ask for routing or timing information 
#        while True: 
#            #print "REMAINING FIELDS: ", fields
#            table, qfields = self.get_platform_max_fields(fields, join) 
#            if not table: 
#                raise Exception, "Cannot complete query: %s" % fields 
#
#            #print "CALLING %s::%s for (%r)" % (table.platform, table.name, fields)
#
#            #q = Query(fact_table, ts, {}, list(p['fields'])) 
#            #gateways = MetadataGateways(self.api, {'platform': p['platform']}) 
#            #if not gateways: 
#            #    raise Exception, "No gateway found for platform '%(platform)s'" % p 
#            #config = json.loads(gateways[0]['config']) 
#
#            ## We add the caller to the config parameter 
#            #config['caller'] = self.caller 
#
#            # We need the key to perform the join
#            key = table.keys[0]
#            qfields.add(key)
#            if not join: 
#                qp = qp.From(table, qfields) 
#                join = True 
#            else: 
#                r = AST(self).From(table, qfields) 
#                # we join on hostname (hardcoded) 
#                qp = qp.join(r, key)
#
#            # Remove the fields we obtain from the ones yet to be queried
#            for qf in qfields:
#                fields = [f for f in fields if not f == qf and not f.startswith("%s." % qf)]
#                #if qf in fields: 
#                #    fields.remove(qf) 
#
#            # Stop if we have no more fields to query
#            if not fields: 
#                break 
#
#        # Now we apply the operators
#        qp = qp.selection(query.filters) 
#        qp = qp.projection(query.fields) 
#        #qp = qp.sort(query.get_sort()) 
#        #qp = qp.limit(query.get_limit()) 
#
#        return qp
#        #return list(qp._get()) 


    #def cb(self, value):
    #    print "callback"
    #    if not value:
    #        print "LAST VALUE =========================================="
    #        self.event.set()
    #    self.results.append(value)

    def do_forward(self, query, route, deferred, execute=True, user=None):
        """
        Effectively runs the forwarding of the query to the route
        """

        # the router parameter is ignored until we clearly state what are the
        # different entities of a router and their responsabilities

        
        # Parameter route is ignored temporarily, we compute it naively...

        try:
            def attribute_closure(attributes, fd_set):
                """
                Compute the closure of a set of attributes under the set of functional dependencies fd_set
                """
                if not isinstance(attributes, (set, frozenset)):
                    closure = set([attributes])
                else:
                    closure = set(attributes)
                while True:
                    old_closure = closure.copy()
                    for y, z in fd_set: # Y -> Z
                        if y in closure:
                            closure.add(z)
                    if old_closure == closure:
                        break
                return closure
    
            def fd_minimal_cover(fd_set):
                # replace each FD X -> (A1, A2, ..., An) by n FD X->A1, X->A2, ..., X->An
                min_cover = set([(y, attr) for y, z in fd_set for attr in z if y != attr])
                for x, a in min_cover.copy():
                    reduced_min_cover = set([fd for fd in min_cover if fd != (x,a)])
                    x_plus = attribute_closure(x, reduced_min_cover)
                    if a in x_plus:
                        min_cover = reduced_min_cover
                for x, a in min_cover:
                    if isinstance(x, frozenset):
                        for b in x:
                            # Compute (X-B)+ with respect to (G-(X->A)) U ((X-B)->A) = S
                            x_minus_b = frozenset([i for i in x if i != b])
                            s = set([fd for fd in min_cover if fd != (x,a)])
                            s.add((x_minus_b, a))
                            x_minus_b_plus = attribute_closure(x_minus_b, s) 
                            if b in x_minus_b_plus:
                                reduced_min_cover = set([fd for fd in min_cover if fd != (x,a)])
                                min_cover = reduced_min_cover
                                min_cover.add( (x_minus_b, a) )
                return min_cover

            def to_3nf(tables):
                # Build the set of functional dependencies
                fd_set = set([(key, g.fields) for g in tables for key in g.keys])

                # Find a minimal cover
                fd_set = fd_minimal_cover(fd_set)
                
                # For each set of FDs in G of the form (X->A1, X->A2, ... X->An)
                # containing all FDs in G with the same determinant X ...
                determinants = {}
                for x, a in fd_set:
                    if not x in determinants:
                        determinants[x] = set([])
                    determinants[x].add(a)

                # ... create relaton R = (X, A1, A2, ..., An)
                relations = []
                for x, y in determinants.items():
                    # Platform list and names for the corresponding key x and values
                    sources = [t for t in tables if x in t.keys]
                    p = [s.platform for s in sources]
                    if len(p) == 1: p = p[0]
                    n = list(sources)[0].name
                    # Note, we do not manage multiple keys here...d
                    fields = list(y)
                    if isinstance(x, frozenset):
                        fields.extend(list(x))
                    else:
                        fields.append(x)
                    t = Table(p, n, fields, [x])
                    relations.append(t)
                return relations

            def build_Gnf(tables_3nf):
                G_nf = nx.DiGraph() 
                for table in tables_3nf:
                    sources = [t for t in tables if list(table.keys)[0] in t.keys]
                    G_nf.add_node(table, {'sources': sources})

                    # We loop through the different _nodes_ of the graph to see whether
                    # we need to establish some links
                    for node, data in G_nf.nodes(True):
                        if node == table: # or set(node.keys) & set(table.keys):
                            continue

                        # Another table is pointing to the considered _table_:
                        # FK -> local.PK
                        link = False
                        for k in table.keys:
                            # Checking for the presence of each key of the table in previously inserted tables
                            if isinstance(k, frozenset):
                                if set(k) <= set(node.fields): # Multiple key XXX
                                    link = True
                            else:
                                if k in node.fields:
                                    link = True
                        if link:
                            #print "EDGE: %s -> %s" % (node, table)
                            G_nf.add_edge(node, table, {'cost': True})
                        
                        # The considered _table_ has a pointer to the primary key of another table
                        # local.FK -> PK
                        link = False
                        # Testing for each possible key of the _node_
                        for k in node.keys:
                            if isinstance(k, frozenset):
                                if set(k) <= set(t.fields): # Multiple key XXX
                                    link = True
                            else:
                                # the considered key _k_ is a simple field
                                if k in table.fields:
                                    link = True

                        if link:
                            #print "EDGE: %s -> %s" % (table, node)
                            G_nf.add_edge(table, node, {'cost': True})

                        # If _table_ names the object _node_ 1..N (or 1..1)
                        if node.name in table.fields:
                            G_nf.add_edge(table, node, {'cost': True, 'type': '1..N'})

                return G_nf

            def get_tree_edges(G_nf, root):
                return [e for e in dfs_edges(G_nf, root)]

            def get_root(G_nf, query):
                # Let's extract the query tree rooted at the fact table
                root = [node[0] for node in G_nf.nodes(True) if node[0].name == query.fact_table]
                if not root:
                    raise Exception, "no root found"
                #print "root=", root
                root = root[0]
                return root
    

            def prune_query_tree(tree, tree_edges, nodes, query_fields):
                # *** Compute the query plane ***
                for node in tree.nodes():
                    data = nodes[node]
                    if 'visited' in data and data['visited']:
                        break;
                    if (set(query_fields) & set(node.fields)):
                        # mark all nodes until we reach the root (no pred) or a marked node
                        cur_node = node
                        # XXX DiGraph.predecessors_iter(n)
                            #link = True
                        while True:
                            if 'visited' in data and data['visited']:
                                break
                            data['visited'] = True
                            pred = tree.predecessors(cur_node)
                            if not pred:
                                break
                            cur_node = pred[0]
                            data = nodes[cur_node]
                visited_tree_edges = [e for e in tree_edges if 'visited' in nodes[e[0]] and 'visited' in nodes[e[1]]]
                #print ["%s %s" % (s,e) for s,e in visited_tree_edges]
                #print "Building tree of visited nodes"
                #tree = nx.DiGraph(visited_tree_edges)
                #return tree
                # clean up
                for node in tree.nodes():
                    if 'visited' in nodes[node]:
                        del nodes[node]['visited']
                return visited_tree_edges
    
            def process_query(query, G_nf, user):
                # We process a single query without caring about 1..N
                # former method
                nodes = dict(G_nf.nodes(True))

                # Builds the query tree rooted at the fact table
                root = get_root(G_nf, query)
                if not root:
                    print "E: Cannot answer query as is: missing root '%s'" % root
                tree_edges = get_tree_edges(G_nf, root)
                if not tree_edges:
                    print "E: Cannot answer the query as is: cannot build tree"
                tree = nx.DiGraph(tree_edges)

                # Plot it
                #nx.draw_graphviz(tree)
                #plt.show()

                # Necessary fields are the one in the query augmented by the keys in the filters
                needed_fields = set(query.fields)
                if query.filters:
                    needed_fields.update(query.filters.keys())

                # Prune the tree from useless tables
                visited_tree_edges = prune_query_tree(tree, tree_edges, nodes, needed_fields)
                #tree = prune_query_tree(tree, tree_edges, nodes, needed_fields)
                if not visited_tree_edges:
                    # The root is sufficient
                    # OR WE COULD NOT ANSWER QUERY
                    q = Query(fact_table=root.name, filters=query.filters, fields=needed_fields)
                    return AST(self, user).From(root, q) # root, needed_fields)

                qp = None
                root = True
                for s, e in visited_tree_edges:
                    # We start at the root if necessary
                    if root:
                        local_fields = set(needed_fields) & s.fields
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
                            q = Query(fact_table=max_table.name, filters=query.filters, fields=list(max_fields))
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
                    # We add fields necessary for performing joins = keys of all the children
                    # XXX does not work for multiple keys
                    #for ss,ee in visited_tree_edges:
                    #    print "SS/EE", ss, ee
                    #    if ss == e: # or ee (node) inherits from ss (resource), 
                    #        # XXX Here we are reasoning on the table name, while
                    #        # previously it was on the keys only
                    #        print "ADDING KEY", ee.keys
                    #        local_fields.update(ee.keys)
                    # Adding key for the join
                    local_fields.update(e.keys)

                    # We adopt a greedy strategy to get the required fields (temporary)
                    # We assume there are no partitions
                    first_join = True
                    left = AST(self, user)
                    sources = nodes[e]['sources'][:]
                    while True:
                        max_table, max_fields = get_table_max_fields(local_fields, sources)
                        if not max_table:
                            break;
                        q = Query(fact_table=max_table.name, filters=query.filters, fields=list(max_fields))
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
                

            def process_subqueries(query, G_nf, user):
                qp = AST(self, user)

                cur_filters = []
                cur_params = []
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

                # TODO params

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

                        child_ast = process_subqueries(subquery, G_nf, user)
                        children_ast.append(child_ast.root)

                    parent = Query(query.action, query.fact_table, cur_filters, cur_params, cur_fields)
                    parent_ast = process_query(parent, G_nf, user)
                    qp = parent_ast
                    qp.subquery(children_ast)
                else:
                    parent = Query(query.action, query.fact_table, cur_filters, cur_params, cur_fields)
                    qp = process_query(parent, G_nf, user)
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

            def run_query(query):
                output = []
                return output

            # BUILD THE QUERY PLAN
            #######################

            tables = self.rib.keys() # HUM
            tables_3nf = to_3nf(tables)

            # Creates a join graph from the tables in normalized format
            G_nf = build_Gnf(tables_3nf)

            # Plot it
            #nx.draw_graphviz(G_nf)
            #plt.show()

            qp = process_subqueries(query, G_nf, user)

        except Exception ,e:
            print "Exception in do_forward", e
            traceback.print_exc()
            return []
            
        # We should now have a query plan
        print ""
        print "QUERY PLAN:"
        print "-----------"
        qp.dump()
        print ""
        print ""

        if not execute:
            return None
        print "I: Install query plan."
        d = defer.Deferred() if deferred else None
        cb = Callback(d, self.event)
        qp.callback = cb
        qp.start()

        #self.sourcemgr.run()

        if deferred:
            return d

        self.event.wait()
        self.event.clear()
        return cb.results


class THRouter(THLocalRouter, Router):
    pass


