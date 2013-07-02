from types import StringTypes 
from sfa.rspecs.rspec import RSpec
from sfa.util.xrn import Xrn, get_leaf
from manifold.gateways.sfa.rspecs import RSpecParser
from manifold.core.filter import Filter
from sfa.util.xml import XpathFilter
# for debug
from manifold.util.log import Log
import pprint

# Maps properties within an element of a RSpec to entries in the returned
# dictionary
#    RSPEC_ELEMENT
#        rspec_property -> dictionary_property
MAP = {
    'node': {
        'location.country': 'country',
        'location.latitude': 'latitude',
        'location.longitude': 'longitude',
        'sliver.name': 'sliver',
        'position_3d.x': 'x',
        'position_3d.y': 'y',
        'position_3d.z': 'z',
        'granularity.grain': 'granularity', # harmonize between ple and nitos
    },
    'spectrum/channel': {
    }
}

# The key for resources, used for leases
RESOURCE_KEY = 'hrn'

# HOOKS TO RUN OPERATIONS ON GIVEN FIELDS
def channel_urn_hrn_exclusive(value):
    output = {}
    # XXX HARDCODED FOR NITOS
    xrn = Xrn('%(network)s.nitos.channel.%(channel_num)s' % value, type='channel')
    return {'resource_urn': xrn.urn, 'resource_hrn': xrn.hrn, 'exclusive': True}

#   RSPEC_ELEMENT
#       rspec_property -> dictionary that is merged when we encounter this
#                         property (the property value is passed as an argument)
#       '*' -> dictionary merged at the end, useful to add some properties made
#              from the combination of several others (the full dictionary is
#              passed as an argument)
HOOKS = {
    'node': {
        'component_id': lambda value : {'resource_hrn': Xrn(value).get_hrn(), 'resource_urn': value}
    },
    'link': {
        'component_id': lambda value : {'resource_hrn': Xrn(value).get_hrn(), 'resource_urn': value}       
    },
    'spectrum/channel': {
        '*': lambda value: channel_urn_hrn_exclusive(value)
    },
    '*': {
        'exclusive': lambda value: {'exclusive': value.lower() not in ['false']}
    }
}

# END HOOKS

class SFAv1Parser(RSpecParser):

    def __init__(self, *args):
        if len(args) == 1:
            rspec = args[0]
            self.rspec = RSpec(rspec).version
        elif len(args) == 2:
            resources = args[0]
            leases = args[1]
            self.resources_by_network = {}
            for r in resources:
                val = r['resource_urn'] if isinstance(r, dict) else r
                auth = Xrn(val).authority
                if not auth: raise Exception, "No authority in specified URN %s" % val
                Log.tmp(auth)
                network = auth[0]
                if not network in self.resources_by_network:
                    self.resources_by_network[network] = []
                self.resources_by_network[network].append(val)

            self.leases_by_network = {}
            for l in leases:
                # @loic Corrected bug
                val = l['resource_urn'] if isinstance(l, dict) else l[0]
                #val = l['resource_urn'] if isinstance(l, dict) else r
                auth = Xrn(val).authority
                if not auth: raise Exception, "No authority in specified URN"
                network = auth[0]
                Log.tmp(auth)
                if not network in self.leases_by_network:
                    self.leases_by_network[network] = []
                self.leases_by_network[network].append(l)

    def prop_from_elt(self, element, prefix = ''):
        """
        Returns a property or a set of properties
        {key: value} or {key: (value, unit)}
        """
        ret = {}
        if prefix: prefix = "%s." % prefix
        tag = element.tag

        # Analysing attributes
        for k, v in element.attrib.items():
            ret["%s%s.%s" % (prefix, tag, k)] = v

        # Analysing the tag itself
        if element.text:
            ret["%s%s" % (prefix, tag)] = element.text

        # Analysing subtags
        for c in element.getchildren():
            ret.update(self.prop_from_elt(c, prefix=tag))

        # XXX special cases:
        # - tags
        # - units
        # - lists

        return ret

    def dict_from_elt(self, network, element):
        """
        Returns an object
        """
        ret = {}
        ret['type'] = element.tag
        ret['network'] = network
        for k, v in element.attrib.items():
            #print "%s = %s" % (k,v)
            ret[k] = v
        for c in element.getchildren():
            ret.update(self.prop_from_elt(c))
        return ret

    def dict_rename(self, dic, name):
        """
        Apply map and hooks
        """
        # XXX We might create substructures if the map has '.'
        ret = {}
        for k, v in dic.items():
            if name in MAP and k in MAP[name]:
                ret[MAP[name][k]] = v
            else:
                ret[k] = v
            if name in HOOKS and k in HOOKS[name]:
                ret.update(HOOKS[name][k](v))
            if '*' in HOOKS and k in HOOKS['*']:
                ret.update(HOOKS['*'][k](v))
        if name in HOOKS and '*' in HOOKS[name]:
            ret.update(HOOKS[name]['*'](ret))
        return ret

    def parse_element(self, name,network=None):
        if network is None:
            # TODO: use SFA Wrapper library
            # self.rspec.version.get_nodes()
            # self.rspec.version.get_links()
            elements = self.rspec.xml.xpath("//default:%s | //%s" %(name,name))
            if self.network is not None:
                elements = [self.dict_from_elt(self.network, n.element) for n in elements]
        else:
            elements = self.rspec.xml.xpath("/RSpec/network[@name='%s']/%s" % (network, name))
            elements = [self.dict_from_elt(network, n.element) for n in elements]
        if name in MAP:
            elements = [self.dict_rename(n, name) for n in elements]
        return elements

    def to_dict(self, version):
        networks = self.rspec.xml.xpath('/RSpec/network/@name')
        networks = [str(n.element) for n in networks]
        # @loic Added for GENIv3 with no networks
        if not networks:
            networks = []
            # specify the network from GetVersion if not explicit in the Rspec
            if 'hrn' in version:
                self.network = version['hrn']
            else:
                self.network = None
            resources=self.dict_resources()
            leases=self.dict_leases(resources)
            #print "RESOURCES RESULTS FOR GENI v3 = "
            #pprint.pprint(resources)
        else:
            # @loic Functions for resources and leases created
            for network in networks:
                resources=self.dict_resources(network)
                leases=self.dict_leases(resources,network)
        return {'resource': resources, 'lease': leases}

    # @loic Added function dict_resources
    def dict_resources(self,network=None):
        result=[]
        # NODES / CHANNELS / LINKS
        for type in ['node', 'spectrum/channel', 'link']:
            result.extend(self.parse_element(type,network))
        return result

    # @loic Added function dict_resources
    def dict_leases(self,resources,network=""):
        result=[]
        # LEASES
        lease_elems = self.rspec.xml.xpath("/RSpec/network[@name='%s']/lease" % network)
        for l in lease_elems:
           lease = dict(l.element.attrib)
           for resource_elem in l.element.getchildren():
                rsrc_lease = lease.copy()
                filt = self.dict_from_elt(network, resource_elem)
                match = Filter.from_dict(filt).filter(resources)
                if len(match) == 0:
                   #print "E: Ignored lease with no match:", filt
                   continue
                if len(match) > 1:
                   #print "E: Ignored lease with multiple matches:", filt
                   continue
                match = match[0]

                # Check whether the node is reservable
                # Check whether the node has some granularity
                if not 'exclusive' in match:
                    #print "W: No information about reservation capabilities of the node:", filt
                    pass
                else:
                    if not match['exclusive']:
                       print "W: lease on a non-reservable node:", filt
                if not 'granularity' in match:
                    #print "W: Granularity not present in node:", filt
                    pass
                else:
                    rsrc_lease['granularity'] = match['granularity']
                if not 'resource_urn' in match:
                    #print "E: Ignored lease with missing 'resource_urn' key:", filt
                    continue
                rsrc_lease['resource_urn'] = match['resource_urn']
                rsrc_lease['network'] = Xrn(match['resource_urn']).authority[0]
                rsrc_lease['hrn'] = Xrn(match['resource_urn']).hrn
                rsrc_lease['type'] = Xrn(match['resource_urn']).type
                result.append(rsrc_lease)
        return result
        #print ""
        #print "========================================"
        #print "PARSING RSPECS leases:"
        #print "-----------------------"
        #for l in leases:
        #    print l
        #print "======="

    def rspec_add_header(self, rspec):
        rspec.append('<?xml version="1.0"?>')
        rspec.append('<RSpec type="SFA">') # expires="2012-10-07T02:24:53Z" generated="2012-10-07T01:24:53Z">')

    def rspec_add_footer(self, rspec):
        rspec.append('</RSpec>')

    def rspec_add_networks(self, slice_id, rspec):
        networks = set(self.resources_by_network.keys()).union(set(self.leases_by_network.keys()))
        for n in networks:
            self.rspec_add_network(slice_id, rspec, n)

    def rspec_add_network(self, slice_id, rspec, network):
        rspec.append('  <network name="%s">' % network)

        if network in self.resources_by_network:
            for r in self.resources_by_network[network]:
                rspec.append('    <node component_id="%s">' % r)
                rspec.append('      <sliver/>')
                rspec.append('    </node>')

        if network in self.leases_by_network:
        
            # Group leases by (start_time, duration)
            lease_groups = {}
            for l in self.leases_by_network[network]: # Do we need to group ?
                if isinstance(l, list):
                    print "W: list to dict for lease"
                    l = {'resource_urn': l[0], 'slice_id': slice_id, 'start_time': l[1], 'duration': l[2]}
                lease_tuple = (l['start_time'], l['duration'])
                if lease_tuple in lease_groups:
                    lease_groups[lease_tuple].append(l)
                else:
                    lease_groups[lease_tuple] = [l]
                    
            # Create RSpec content
            for lease_tuple, leases in lease_groups.items():
                rspec.append('    <lease slice_id="%s" start_time="%s" duration="%s">' % (slice_id, lease_tuple[0], lease_tuple[1]))
                for l in leases:
                    type = Xrn(l['resource_urn']).type
                    if type == 'node':
                        rspec.append('    <node component_id="%s"/>' % l['resource_urn'])
                    elif type == 'channel':
                        rspec.append('    <channel channel_num="%s"/>' % get_leaf(l['resource_urn']))
                    else:
                        print "W: Ignore element while building rspec"
                        continue 
                rspec.append('    </lease>')

        rspec.append('  </network>')

    def to_rspec(self, slice_id):
            
        rspec = []
        self.rspec_add_header(rspec)
        self.rspec_add_networks(slice_id, rspec)
        self.rspec_add_footer(rspec)
        return "\n".join(rspec)
