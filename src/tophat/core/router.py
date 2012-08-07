import os, sys
import xml.etree.cElementTree as ElementTree

import traceback
import threading
from tophat.util.xmldict import *
from tophat.util.reactor_thread import ReactorThread
from tophat.core.filter import Filter
from tophat.router import *
from tophat.core.sourcemgr import SourceManager
from tophat.gateways import *

#from tophat.models import session, Platform

class ParameterError(StandardError): pass

class Table:
    """
    Implements a database table schema.
    """

    def __init__(self, platform, name, fields, keys):
        self.platform = platform
        self.name = name
        self.fields = fields
        self.keys = keys

    def __str__(self):
        return "<Table name='%s' platform='%s' fields='%r' keys='%r'>" % (self.name, self.platform, self.fields, self.keys)

class THDestination(Destination):
    """
    Implements a destination in TopHat 

    = a fact table + a set of tables/fields.

    We assume this is a correct DAG specification.

    1/ A field designates several tables = OR specification.
    2/ The set of fields specifies a AND between OR clauses.

    """
    
    def __init__(self, fact_table, filters, fields):
        self.fact_table = fact_table

        if isinstance(filters, list):
            self.filters = Filter.from_list(filters)
        elif isinstance(filters, dict):
            self.filters = Filter.from_dict(filters)
        else:
            self.filters = filters

        self.fields = fields

    def __str__(self):
        return "<THDestination fact_table='%s' filters='%s' fields='%r'>" % (self.fact_table, self.filters, self.fields)


class THQuery(Query):
    """
    Implements a TopHat query.
    """

    def __init__(self, *args, **kwargs):
        l = len(kwargs.keys())

        # range(x,y) <=> [x, y[
        if len(args) in range(1,4) and type(args) == tuple:
            self.destination = THDestination(*args)
            return
        elif 'destination' in kwargs:
            destination = kwargs['destination']
            if type(destination) != THDestination:
                raise TypeError("Destination of type %s expected in argument. Got %s" % (type(destination), THDestination))
            self.destination = kwargs[destination]
            del kwargs['destination']
            
            if not kwargs: return 
        elif 'fact_table' in kwargs:
            fact_table = kwargs['fact_table']
            del kwargs['fact_table']

            if 'filters' in kwargs:
                filters = kwargs['filters']
                del kwargs['filters']
            else:
                filters = None

            if 'fields' in kwargs:
                fields = kwargs['fields']
                del kwargs['fields']
            else:
                fields = None

            self.destination = THDestination(fact_table, filters, fields)

            if not kwargs: return
        
        raise ParameterError, "Invalid parameter(s) : %r" % kwargs.keys()
        
        super(THQuery, self).__init__(self, destination)

    def __str__(self):
        return "<THQuery destination=%s>" % self.destination

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
        print "calling init"
        self.reactor = ReactorThread()
        self.sourcemgr = SourceManager(self.reactor)
        LocalRouter.__init__(self, Table, object)
        self.event = threading.Event()

    def __enter__(self):
        print "calling enter"
        self.reactor.startReactor()
        return self

    def __exit__(self, type, value, traceback):
        print "calling exit"
        self.reactor.stopReactor()
        print "I: Reactor thread stopped. Waiting for thread to terminate..."
        self.reactor.join()

    def import_file(self, metadata):
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
            fields = [f['field'] for f  in field_arr['field']]

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

    def get_gateway(self, platform, cb, query):
        config_tophat = {
            'url': 'http://api.top-hat.info/API/'
        }
        config_myslice = {
            'url': 'http://api.myslice.info/API/'
        }
        config_ple = {
            'auth': 'ple.upmc',
            'user': 'ple.upmc.slicebrowser',
            'sm': 'http://www.planet-lab.eu:12347/',
            'registry': 'http://www.planet-lab.eu:12345/',
            'user_private_key': '/var/myslice/myslice.pkey',
            'caller': {'email': 'demo'}
        }

        class_map = { 'ple': (SFA, config_ple) , 'tophat': (XMLRPC, config_tophat), 'myslice': (XMLRPC, config_myslice) }

        try:
            cls, conf = class_map[platform]
            return cls(cb, platform, query, conf)
        except KeyError, key:
            raise Exception, "Platform missing '%s'" % key

    def get_static_routes(self, directory):
        for root, dirs, files in os.walk(directory):
            for d in dirs[:]:
                if d[0] == '.':
                    dirs.remove(d)
            metadata = [f for f in files if f[-3:] == 'xml']
            for m in metadata:
                self.import_file(os.path.join(root, m))
        

    def get_platform_max_fields(self, fields, join):
        # Search for the platform::method that allows for the largest number of missing fields
        maxfields = 0
        ret = None
        
        for dest, route in self.rib.items():
            # HACK to make tophat on join
            if not join and dest.platform in ['tophat', 'myslice']:
                continue
            isect = set(dest.fields).intersection(set(fields))
            if len(isect) > maxfields:
                maxfields = len(isect)
                ret = (dest, isect)
        return ret

    def compute_query_plan(self, query):

        # XXX this should be replaced by a Steiner Tree computation

        from tophat.core.ast import AST
        from tophat.core.metadata import Metadata
        from tophat.core.gateway import Gateway

        dest = query.destination
        fact_table, filters, fields = dest.fact_table, dest.filters, dest.fields

        # This method is broken, need to replace it with steiner

        if filters:
            fields.extend(filters.keys())
        fields = set(fields)
         
        # Query plan 
        qp = AST()
        join = False 

        # Note: We could skip or restrict the set of platforms, and ask for routing or timing information 
        while True: 
            #print "REMAINING FIELDS: ", fields
            table, qfields = self.get_platform_max_fields(fields, join) 
            if not table: 
                raise Exception, "Cannot complete query: %s" % fields 

            #print "CALLING %s::%s for (%r)" % (table.platform, table.name, fields)

            #q = Query(fact_table, ts, {}, list(p['fields'])) 
            #gateways = MetadataGateways(self.api, {'platform': p['platform']}) 
            #if not gateways: 
            #    raise Exception, "No gateway found for platform '%(platform)s'" % p 
            #config = json.loads(gateways[0]['config']) 

            ## We add the caller to the config parameter 
            #config['caller'] = self.caller 

            # We need the key to perform the join
            qfields.add('hostname')
            if not join: 
                qp = qp.From(table, qfields) 
                join = True 
            else: 
                r = AST().From(table, qfields) 
                # we join on hostname (hardcoded) 
                qp = qp.join(r, 'hostname') 

            # Remove the fields we obtain from the ones yet to be queried
            for f in qfields:
                if f in fields: 
                    fields.remove(f) 

            # Stop if we have no more fields to query
            if not fields: 
                break 

        # Now we apply the operators
        qp = qp.selection(dest.filters) 
        qp = qp.projection(dest.fields) 
        #qp = qp.sort(query.get_sort()) 
        #qp = qp.limit(query.get_limit()) 

        return qp
        #return list(qp._get()) 


    def install_query_plan(self, qp, callback):
        qp._root.install(self, callback)

    def cb(self, value):
        if not value:
            print "LAST VALUE =========================================="
            self.event.set()
        self.results.append(value)
        
    def do_forward(self, query, route):
        """
        Effectively runs the forwarding of the query to the route
        """

        # the router parameter is ignored until we clearly state what are the
        # different entities of a router and their responsabilities

        
        # Parameter route is ignored temporarily, we compute it naively...

        qp = self.compute_query_plan(query)
        print ""
        print "QUERY PLAN:"
        print "-----------"
        qp.dump()
        print ""
        print ""

        self.install_query_plan(qp, self.cb)
        self.sourcemgr.run()
        self.results = []
        self.event.wait()
        self.event.clear()
        # Go through query plan and setup source and processing nodes...
        # formerly : Installs the query plan = multipipe + active processing = AST + socket handler

        return self.results

class THRouter(THLocalRouter, Router):
    pass


