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

        # These are all resources 
        # get_resources function can return all resources or a specific type of resource
        try:
            resources = rspec.version.get_resources()
        except Exception, e:
            Log.warning("Could not retrieve resources in RSpec: %s" % e)
        
        # XXX does not scale... we need get_resources and that's all
        try:
            nodes = rspec.version.get_nodes()
        except Exception, e:
            nodes = list()
            Log.warning("Could not retrieve nodes in RSpec: %s" % e)
        try:
            leases = rspec.version.get_leases()
        except Exception, e:
            leases = list()
            Log.warning("Could not retrieve leases in RSpec: %s" % e)
        try:
            links = rspec.version.get_links()
        except Exception, e:
            links = list()
            Log.warning("Could not retrieve links in RSpec: %s" % e)
        try:
            channels = rspec.version.get_channels()
        except Exception, e:
            channels = list()
            Log.warning("Could not retrieve channels in RSpec: %s" % e)

        # Extend object and Format object field's name
        for resource in resources:
            resource['urn'] = resource['component_id']

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
            resources.append(cls.make_dict_rec(node))

        # NOTE a channel is a resource and should not be treated independently
        #     resource
        #        |
        #   +----+------+-------+
        #   |    |      |       |
        # node  link  channel  etc.
        #resources.extend(nodes)
        #resources.extend(channels)

        for lease in leases:
            lease['resource'] = lease.pop('component_id')
            lease['slice']    = lease.pop('slice_id')

        return {'resource': resources, 'lease': leases } 
#               'channel': channels \
#               }

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
        if sfa_leases:
            # SFAWRAP BUG ???
            # rspec.version.add_leases bugs with an empty set of leases
            # slice_id = leases[0]['slice_id']
            # TypeError: list indices must be integers, not str
            rspec.version.add_leases((sfa_leases, [])) # XXX Empty channels for now
   
        return rspec.toxml()


    #---------------------------------------------------------------------------
    # RSpec parsing helpers
    #---------------------------------------------------------------------------

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
            sfa_lease['duration']   = lease['duration']
            sfa_leases.append(sfa_lease)
        return sfa_leases
    
class PLEParser(SFAWrapParser):

    @classmethod
    def on_build_resource_hook(cls, resource):
        # PlanetLab now requires a node to have a list of slivers
        print resource
        resource['slivers'] = [{'type': 'plab-vnode'}]
        print "new resource", resource
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
