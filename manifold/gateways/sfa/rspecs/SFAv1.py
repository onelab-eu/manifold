from types import StringTypes 
from xml.etree import ElementTree
from sfa.rspecs.rspec import RSpec
from sfa.util.xrn import Xrn, get_leaf
from manifold.gateways.sfa.rspecs import RSpecParser
from manifold.core.filter import Filter
from sfa.util.xml import XpathFilter
# for debug
from manifold.util.log import Log
import pprint

# TODO We could have namespace specific hooks and maps

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

RESOURCE_TYPES = ['node', 'spectrum/channel', 'link']

# The key for resources, used for leases
RESOURCE_KEY = 'urn' # 'resource_hrn'

# HOOKS TO RUN OPERATIONS ON GIVEN FIELDS
def channel_urn_hrn_exclusive(value):
    output = {}
    # XXX HARDCODED FOR NITOS
    xrn = Xrn('%(network)s.nitos.channel.%(channel_num)s' % value, type='channel')
    return {'urn': xrn.urn, 'resource_hrn': xrn.hrn, 'exclusive': True}

#   RSPEC_ELEMENT
#       rspec_property -> dictionary that is merged when we encounter this
#                         property (the property value is passed as an argument)
#       '*' -> dictionary merged at the end, useful to add some properties made
#              from the combination of several others (the full dictionary is
#              passed as an argument)
HOOKS = {
    'node': {
        'component_id': lambda value : {'resource_hrn': Xrn(value).get_hrn(), 'urn': value}
    },
    'link': {
        'component_id': lambda value : {'resource_hrn': Xrn(value).get_hrn(), 'urn': value}       
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
    # This should be a generic RSpec parser to be used in case of an unknown
    # RSpec. What about metadata ? Can't we have unspecified RSpecs, that has *
    # = we forward all unknown fields == underspecified metadata

    def __init__(self, *args):
        """
        SFAv1Parser(rspec)
        or
        SFAv1Parser(resources, leases)
        """

        # SFAv1Parser(rspec)
        # 
        if len(args) == 1:
            rspec = args[0]
            self.rspec = RSpec(rspec).version

        # SFAv1Parser(resources, leases)
        # 
        elif len(args) == 2:
            resources = args[0]
            leases    = args[1]

            self.resources_by_network = {}
            for r in resources:
                val = r['urn'] if isinstance(r, dict) else r
                auth = Xrn(val).authority
                if not auth: raise Exception, "No authority in specified URN %s" % val
                network = auth[0]
                if not network in self.resources_by_network:
                    self.resources_by_network[network] = []
                self.resources_by_network[network].append(val)

            self.leases_by_network = {}
            for l in leases:
                val = l['urn'] if isinstance(l, dict) else l[0]
                auth = Xrn(val).authority
                if not auth: raise Exception, "No authority in specified URN"
                network = auth[0]
                if not network in self.leases_by_network:
                    self.leases_by_network[network] = []
                self.leases_by_network[network].append(l)

# MAKE A GENERIC PARSER #     def get_element_tag(self, element):
# MAKE A GENERIC PARSER #         tag = element.tag
# MAKE A GENERIC PARSER #         if element.prefix in element.nsmap:
# MAKE A GENERIC PARSER #             # NOTE: None is a prefix that can be in the map (default ns)
# MAKE A GENERIC PARSER #             start = len(element.nsmap[element.prefix]) + 2 # {ns}tag
# MAKE A GENERIC PARSER #             tag = tag[start:]
# MAKE A GENERIC PARSER #         
# MAKE A GENERIC PARSER #         return tag
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #     def prop_from_elt(self, element, prefix = ''):
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         Returns a property or a set of properties
# MAKE A GENERIC PARSER #         {key: value} or {key: (value, unit)}
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         ret = {}
# MAKE A GENERIC PARSER #         if prefix: prefix = "%s." % prefix
# MAKE A GENERIC PARSER #         tag = self.get_element_tag(element)
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         # Analysing attributes
# MAKE A GENERIC PARSER #         for k, v in element.attrib.items():
# MAKE A GENERIC PARSER #             ret["%s%s.%s" % (prefix, tag, k)] = v
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         # Analysing the tag itself
# MAKE A GENERIC PARSER #         if element.text:
# MAKE A GENERIC PARSER #             ret["%s%s" % (prefix, tag)] = element.text
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         # Analysing subtags
# MAKE A GENERIC PARSER #         for c in element.getchildren():
# MAKE A GENERIC PARSER #             ret.update(self.prop_from_elt(c, prefix=tag))
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         # XXX special cases:
# MAKE A GENERIC PARSER #         # - tags
# MAKE A GENERIC PARSER #         # - units
# MAKE A GENERIC PARSER #         # - lists
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         return ret
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #     def dict_from_elt(self, network, element):
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         Returns an object
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         ret            = {}
# MAKE A GENERIC PARSER #         ret['type']    = self.get_element_tag(element)
# MAKE A GENERIC PARSER #         ret['network'] = network
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         for k, v in element.attrib.items():
# MAKE A GENERIC PARSER #             ret[k] = v
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         for c in element.getchildren():
# MAKE A GENERIC PARSER #             ret.update(self.prop_from_elt(c))
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         return ret
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #     def dict_rename(self, dic, name):
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         Apply map and hooks
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         # XXX We might create substructures if the map has '.'
# MAKE A GENERIC PARSER #         ret = {}
# MAKE A GENERIC PARSER #         for k, v in dic.items():
# MAKE A GENERIC PARSER #             if name in MAP and k in MAP[name]:
# MAKE A GENERIC PARSER #                 ret[MAP[name][k]] = v
# MAKE A GENERIC PARSER #             else:
# MAKE A GENERIC PARSER #                 ret[k] = v
# MAKE A GENERIC PARSER #             if name in HOOKS and k in HOOKS[name]:
# MAKE A GENERIC PARSER #                 ret.update(HOOKS[name][k](v))
# MAKE A GENERIC PARSER #             if '*' in HOOKS and k in HOOKS['*']:
# MAKE A GENERIC PARSER #                 ret.update(HOOKS['*'][k](v))
# MAKE A GENERIC PARSER #         if name in HOOKS and '*' in HOOKS[name]:
# MAKE A GENERIC PARSER #             ret.update(HOOKS[name]['*'](ret))
# MAKE A GENERIC PARSER #         return ret
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #     def parse_element(self, resource_type, network=None):
# MAKE A GENERIC PARSER #         print "NETWORK", network
# MAKE A GENERIC PARSER #         if network is None:
# MAKE A GENERIC PARSER #             # TODO: use SFA Wrapper library
# MAKE A GENERIC PARSER #             # self.rspec.version.get_nodes()
# MAKE A GENERIC PARSER #             # self.rspec.version.get_links()
# MAKE A GENERIC PARSER #             XPATH_RESOURCE = "//default:%(resource_type)s | //%(resource_type)s"
# MAKE A GENERIC PARSER #             elements = self.rspec.xml.xpath(XPATH_RESOURCE % locals()) 
# MAKE A GENERIC PARSER #             if self.network is not None:
# MAKE A GENERIC PARSER #                 elements = [self.dict_from_elt(self.network, n.element) for n in elements]
# MAKE A GENERIC PARSER #         else:
# MAKE A GENERIC PARSER #             XPATH_RESOURCE = "/RSpec/network[@name='%(network)s']/%(resource_type)s"
# MAKE A GENERIC PARSER #             elements = self.rspec.xml.xpath(XPATH_RESOURCE % locals())
# MAKE A GENERIC PARSER #             elements = [self.dict_from_elt(network, n.element) for n in elements]
# MAKE A GENERIC PARSER #         print "elements", elements
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         # XXX if network == self.network == None, we might not have a dict here !!!
# MAKE A GENERIC PARSER #         if resource_type in MAP:
# MAKE A GENERIC PARSER #             elements = [self.dict_rename(n, resource_type) for n in elements]
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         return elements
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #     def dict_resources(self, network=None):
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         \brief Returns a list of resources from the specified network (eventually None)
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         result=[]
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         # NODES / CHANNELS / LINKS
# MAKE A GENERIC PARSER #         for type in RESOURCE_TYPES:
# MAKE A GENERIC PARSER #             result.extend(self.parse_element(type, network))
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         return result
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #     def dict_leases(self, resources, network=""):
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         \brief Returns a list of leases from the specified network (eventually None)
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         result=[]
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         # XXX All testbeds that have leases have networks XXX
# MAKE A GENERIC PARSER #         XPATH_LEASE = "/RSpec/network[@name='%(network)s']/lease"
# MAKE A GENERIC PARSER #         lease_elems = self.rspec.xml.xpath(XPATH_LEASE % locals())
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         for l in lease_elems:
# MAKE A GENERIC PARSER #            lease = dict(l.element.attrib)
# MAKE A GENERIC PARSER #            for resource_elem in l.element.getchildren():
# MAKE A GENERIC PARSER #                 rsrc_lease = lease.copy()
# MAKE A GENERIC PARSER #                 filt = self.dict_from_elt(network, resource_elem)
# MAKE A GENERIC PARSER #                 match = Filter.from_dict(filt).filter(resources)
# MAKE A GENERIC PARSER #                 if len(match) == 0:
# MAKE A GENERIC PARSER #                    #print "E: Ignored lease with no match:", filt
# MAKE A GENERIC PARSER #                    continue
# MAKE A GENERIC PARSER #                 if len(match) > 1:
# MAKE A GENERIC PARSER #                    #print "E: Ignored lease with multiple matches:", filt
# MAKE A GENERIC PARSER #                    continue
# MAKE A GENERIC PARSER #                 match = match[0]
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #                 # Check whether the node is reservable
# MAKE A GENERIC PARSER #                 # Check whether the node has some granularity
# MAKE A GENERIC PARSER #                 if not 'exclusive' in match:
# MAKE A GENERIC PARSER #                     #print "W: No information about reservation capabilities of the node:", filt
# MAKE A GENERIC PARSER #                     pass
# MAKE A GENERIC PARSER #                 else:
# MAKE A GENERIC PARSER #                     if not match['exclusive']:
# MAKE A GENERIC PARSER #                        print "W: lease on a non-reservable node:", filt
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #                 if not 'granularity' in match:
# MAKE A GENERIC PARSER #                     #print "W: Granularity not present in node:", filt
# MAKE A GENERIC PARSER #                     pass
# MAKE A GENERIC PARSER #                 else:
# MAKE A GENERIC PARSER #                     rsrc_lease['granularity'] = match['granularity']
# MAKE A GENERIC PARSER #                 if not 'urn' in match:
# MAKE A GENERIC PARSER #                     #print "E: Ignored lease with missing 'resource_urn' key:", filt
# MAKE A GENERIC PARSER #                     continue
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #                 rsrc_lease['urn']          = match['urn']
# MAKE A GENERIC PARSER #                 rsrc_lease['network']      = Xrn(match['urn']).authority[0]
# MAKE A GENERIC PARSER #                 rsrc_lease['hrn']          = Xrn(match['urn']).hrn
# MAKE A GENERIC PARSER #                 rsrc_lease['type']         = Xrn(match['urn']).type
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #                 result.append(rsrc_lease)
# MAKE A GENERIC PARSER #         return result
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #     def to_dict(self, version):
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         \brief Converts a RSpec to two lists of resources and leases.
# MAKE A GENERIC PARSER #         \param version Output of the GetVersion() call holding the hrn of the
# MAKE A GENERIC PARSER #         authority. This will be used for testbeds not adding this hrn inside
# MAKE A GENERIC PARSER #         the RSpecs.
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         This function is the entry point for resource parsing. 
# MAKE A GENERIC PARSER #         """
# MAKE A GENERIC PARSER #         Log.tmp("TO DICT")
# MAKE A GENERIC PARSER #         networks = self.rspec.xml.xpath('/RSpec/network/@name')
# MAKE A GENERIC PARSER #         networks = [str(n.element) for n in networks]
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         if not networks:
# MAKE A GENERIC PARSER #             # NOTE: GENI aggregate for example do not add the network alongside
# MAKE A GENERIC PARSER #             # the resources
# MAKE A GENERIC PARSER #             networks = []
# MAKE A GENERIC PARSER #             # We might retrieve the network from GetVersion() if it is not
# MAKE A GENERIC PARSER #             # explicit in the RSpec
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #             # XXX Jordan: do we really need to store it in self ? I would pass it as a parameter
# MAKE A GENERIC PARSER #             # I found the answer: it's because the XPATH expression is different if the RSpec has
# MAKE A GENERIC PARSER #             # the network or not
# MAKE A GENERIC PARSER #             self.network = version.get('hrn')
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #             resources = self.dict_resources()
# MAKE A GENERIC PARSER #             leases    = self.dict_leases(resources)
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         else:
# MAKE A GENERIC PARSER #             # NOTE: A resource might have several networks (eg. from a SM)
# MAKE A GENERIC PARSER #             for network in networks:
# MAKE A GENERIC PARSER #                 resources = self.dict_resources(network)
# MAKE A GENERIC PARSER #                 leases    = self.dict_leases(resources,network)
# MAKE A GENERIC PARSER # 
# MAKE A GENERIC PARSER #         return {'resource': resources, 'lease': leases}


    #---------------------------------------------------------------------------
    # RSpec construction helpers
    #---------------------------------------------------------------------------

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
                    l = {'urn': l[0], 'slice_id': slice_id, 'start_time': l[1], 'duration': l[2]}
                lease_tuple = (l['start_time'], l['duration'])
                if lease_tuple in lease_groups:
                    lease_groups[lease_tuple].append(l)
                else:
                    lease_groups[lease_tuple] = [l]
                    
            # Create RSpec content
            for lease_tuple, leases in lease_groups.items():
                rspec.append('    <lease slice_id="%s" start_time="%s" duration="%s">' % (slice_id, lease_tuple[0], lease_tuple[1]))
                for l in leases:
                    type = Xrn(l['urn']).type
                    if type == 'node':
                        rspec.append('    <node component_id="%s"/>' % l['urn'])
                    elif type == 'channel':
                        rspec.append('    <channel channel_num="%s"/>' % get_leaf(l['urn']))
                    else:
                        print "W: Ignore element while building rspec"
                        continue 
                rspec.append('    </lease>')

        rspec.append('  </network>')

    def to_rspec(self, slice_id):
        """
        \brief Builds a RSpec from the current class
        \param slice_id ???

        This function uses 'self.resources_by_network' and 'self.leases_by_network'
        """
            
        rspec = []
        self.rspec_add_header(rspec)
        self.rspec_add_networks(slice_id, rspec)
        self.rspec_add_footer(rspec)
        return "\n".join(rspec)
