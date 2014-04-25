# RSpec middleware that is in charge of handling rspec parsing and creation through SFAWrap

from sfa.util.xrn                       import Xrn, get_leaf, get_authority, hrn_to_urn, urn_to_hrn
from manifold.gateways.sfa.rspecs       import RSpecParser
from manifold.util.log                  import Log
from sfa.rspecs.rspec                   import RSpec
from types                              import StringTypes

class SFAWrapParser(RSpecParser):

    #---------------------------------------------------------------------------
    # RSpec parsing
    #---------------------------------------------------------------------------

    @classmethod
    def make_dict_rec(cls, obj):
        if not obj or isinstance(obj, (StringTypes, bool)):
            return obj
        if isinstance(obj, list):
            objcopy = []
            for x in obj:
                objcopy.append(cls.make_dict_rec(x))
            return objcopy
        # We thus suppose we have a child of dict
        objcopy = {}
        for k, v in obj.items():
            objcopy[k] = cls.make_dict_rec(v)
        return objcopy


    @classmethod
    def parse(cls, rspec, rspec_version='GENI 3'):
        resources   = list()
        leases      = list()

        rspec = RSpec(rspec, version=rspec_version)

        resources   = cls._get_resources(rspec)
        nodes       = cls._get_nodes(rspec)
        channels    = cls._get_channels(rspec)
        links       = cls._get_links(rspec)
        leases      = cls._get_leases(rspec)
        
        resources.extend(cls._process_resources(resources))
        resources.extend(cls._process_nodes(nodes))
        resources.extend(cls._process_channels(channels))
        resources.extend(cls._process_links(links))

        leases = cls._process_leases(leases)

        return {'resource': resources, 'lease': leases } 

    #---------------------------------------------------------------------------
    # RSpec construction
    #---------------------------------------------------------------------------

    @classmethod
    def build_rspec(cls, slice_urn, resources, leases, rspec_version='GENI 3 request'):
        """
        Builds a RSpecs based on the specified resources and leases.

        Args:
            slice_urn (string) : the urn of the slice [UNUSED] [HRN in NITOS ?]

        Returns:
            string : the string version of the created RSpec.
        """
        
        rspec = RSpec(version=rspec_version)

        nodes = []
        channels = []
        links = []

        # XXX Here it is only about mappings and hooks between ontologies

        for urn in resources:
            # XXX TO BE CORRECTED, this handles None values
            if not urn:
                continue
            resource = dict()
            # TODO: take into account the case where we send a dict of URNs without keys
            #resource['component_id'] = resource.pop('urn')
            resource['component_id'] = urn
            resource_hrn, resource_type = urn_to_hrn(urn) # resource['component_id'])
            # build component_manager_id
            top_auth = resource_hrn.split('.')[0]
            cm = urn.split("+")
            resource['component_manager_id'] = "%s+%s+authority+cm" % (cm[0],top_auth)

            print "resource_type", resource_type
            if resource_type == 'node':
                print "NODE", resource, cls
                resource = cls.on_build_resource_hook(resource)
                nodes.append(resource)
            elif resource_type == 'link':
                links.append(resource)
            elif resource_type == 'channel':
                channels.append(resource)
            else:
                raise Exception, "Not supported type of resource" 

        for node in nodes:
            print "NODE:", node

        rspec.version.add_nodes(nodes, rspec_content_type="request")

        #rspec.version.add_links(links)
        #rspec.version.add_channels(channels)

        sfa_leases = cls.manifold_to_sfa_leases(leases, slice_urn)
        print "sfa_leases", sfa_leases
        if sfa_leases:
            # SFAWRAP BUG ???
            # rspec.version.add_leases bugs with an empty set of leases
            # slice_id = leases[0]['slice_id']
            # TypeError: list indices must be integers, not str
            rspec.version.add_leases(sfa_leases, []) # XXX Empty channels for now
   
        return rspec.toxml()


    #---------------------------------------------------------------------------
    # RSpec parsing helpers
    #---------------------------------------------------------------------------

    # We split those basic functions so that they can be easily overloaded in children classes

    @classmethod
    def _get_resources(cls, rspec):
        # These are all resources 
        # get_resources function can return all resources or a specific type of resource
        try:
            return rspec.version.get_resources()
        except Exception, e:
            return list()

    @classmethod
    def _get_nodes(cls, rspec):
        # XXX does not scale... we need get_resources and that's all
        try:
            return rspec.version.get_nodes()
        except Exception, e:
            #Log.warning("Could not retrieve nodes in RSpec: %s" % e)
            return list()

    @classmethod
    def _get_leases(cls, rspec):
        try:
            return rspec.version.get_leases()
        except Exception, e:
            #Log.warning("Could not retrieve leases in RSpec: %s" % e)
            return list()

    @classmethod
    def _get_links(cls, rspec):
        try:
            return rspec.version.get_links()
        except Exception, e:
            #Log.warning("Could not retrieve links in RSpec: %s" % e)
            return list()

    @classmethod
    def _get_channels(cls, rspec):
        try:
            return rspec.version.get_channels()
        except Exception, e:
            #Log.warning("Could not retrieve channels in RSpec: %s" % e)
            return list()

    @classmethod
    def _process_resources(cls, resources):
        ret = list()

        for resource in resources:
            resource['urn'] = resource['component_id']
            ret.append(resource)
        
        return ret

    @classmethod
    def _process_nodes(cls, nodes):
        ret = list()

        for node in nodes:
            node['type'] = 'node'
            node['network_hrn'] = Xrn(node['component_id']).authority[0] # network ? XXX
            node['hrn'] = urn_to_hrn(node['component_id'])[0]
            node['urn'] = node['component_id']
            node['hostname'] = node['component_name']
            node['initscripts'] = node.pop('pl_initscripts')
            if 'exclusive' in node and node['exclusive']:
                node['exclusive'] = node['exclusive'].lower() == 'true'

            # XXX This should use a MAP as before
            if 'position' in node: # iotlab
                node['x'] = node['position']['posx']
                node['y'] = node['position']['posy']
                node['z'] = node['position']['posz']
                del node['position']

            if 'location' in node:
                if node['location']:
                    node['latitude'] = node['location']['latitude']
                    node['longitude'] = node['location']['longitude']
                del node['location']

            # Flatten tags
            if 'tags' in node:
                if node['tags']:
                    for tag in node['tags']:
                        node[tag['tagname']] = tag['value']
                del node['tags']

            
            # We suppose we have children of dict that cannot be serialized
            # with xmlrpc, let's make dict
            ret.append(cls.make_dict_rec(node))
        return ret

    @classmethod
    def _process_channels(cls, channels):
        return channels

    @classmethod
    def _process_links(cls, links):
        return links

    @classmethod
    def _process_leases(cls, leases):
        ret = list()

        for lease in leases:
            lease['resource'] = lease.pop('component_id')
            lease['slice']    = lease.pop('slice_id')
            if not 'end_time' in lease and set(lease.keys()) <= set(['start_time', 'duration']):
                lease['end_time'] = lease['start_time'] + lease['duration'] * cls.get_grain()
            elif not 'duration' in lease and  set(lease.keys()) <= set(['start_time', 'end_time']):
                lease['duration'] = (lease['end_time'] - lease['start_time']) / cls.get_grain()
            ret.append(lease)

        return ret

    #---------------------------------------------------------------------------
    # RSpec construction helpers
    #---------------------------------------------------------------------------

    @classmethod
    def on_build_resource_hook(cls, resource):
        return resource

    @classmethod
    def manifold_to_sfa_leases(cls, leases, slice_urn):
        sfa_leases = []
        for lease in leases:
            sfa_lease = dict()
            # sfa_lease_id = 
            sfa_lease['component_id'] = lease['resource']
            sfa_lease['slice_id']     = slice_urn
            sfa_lease['start_time']   = lease['start_time']
            
            grain = cls.get_grain() # in seconds
            min_duration = cls.get_min_duration() # in seconds
    
            # We either need end_time or duration
            # end_time is choosen if both are specified !
            if 'end_time' in lease:
                sfa_lease['end_time'] = lease['end_time']
                duration =  (lease['end_time'] - lease['start_time']) / grain
                if duration < min_duration:
                    raise Exception, 'duration < min_duration'
                sfa_lease['duration'] = duration
            elif 'duration' in lease:
                sfa_lease['duration'] = lease['duration']
                sfa_lease['end_time'] = lease['start_time'] + lease['duration']
            else:
                raise Exception, 'Lease not specifying neither end_time nor duration'
            sfa_leases.append(sfa_lease)
        return sfa_leases

    @classmethod
    def get_grain(cls):
        """
        in seconds
        """
        return 1800

    @classmethod
    def get_min_duration(cls):
        return 0
        
    
class PLEParser(SFAWrapParser):

    @classmethod
    def on_build_resource_hook(cls, resource):
        # PlanetLab now requires a node to have a list of slivers
        resource['slivers'] = [{'type': 'plab-vnode'}]
        return resource

class NITOSParser(SFAWrapParser):

    @classmethod
    def on_build_resource_hook(cls, resource):
        resource['granularity'] = {'grain': 1800}
        return resource

class WiLabtParser(SFAWrapParser):

    @classmethod
    def on_build_resource_hook(cls, resource):
        resource['client_id'] = "PC"
        resource['sliver_type'] = "raw-pc"
        return resource

class IoTLABParser(SFAWrapParser):

    @classmethod
    def on_build_resource_hook(cls, resource):
        # XXX How do we know valid sliver types ?
        resource['slivers'] = [{'type': 'iotlab-node'}]
        return resource

    @classmethod
    def get_grain(cls):
        return 60 # s

    @classmethod
    def get_min_duration(cls):
        return 10 * cls.get_grain()
        # XXX BTW, can we do duration = 61 s, or shall it be a multiple of min_duration ???
