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

    def get_element_tag(self, element):
        tag = element.tag
        if element.prefix in element.nsmap:
            # NOTE: None is a prefix that can be in the map (default ns)
            start = len(element.nsmap[element.prefix]) + 2 # {ns}tag
            tag = tag[start:]
        
        return tag

    def prop_from_elt(self, element, prefix = ''):
        """
        Returns a property or a set of properties
        {key: value} or {key: (value, unit)}
        """
        ret = {}
        if prefix: prefix = "%s." % prefix
        tag = self.get_element_tag(element)

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
        ret            = {}
        ret['type']    = self.get_element_tag(element)
        ret['network'] = network

        for k, v in element.attrib.items():
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

    def parse_element(self, resource_type, network=None):
        if network is None:
            # TODO: use SFA Wrapper library
            # self.rspec.version.get_nodes()
            # self.rspec.version.get_links()
            XPATH_RESOURCE = "//default:%(resource_type)s | //%(resource_type)s"
            elements = self.rspec.xml.xpath(XPATH_RESOURCE % locals()) 
            if self.network is not None:
                elements = [self.dict_from_elt(self.network, n.element) for n in elements]
        else:
            XPATH_RESOURCE = "/RSpec/network[@name='%(network)s']/%(resource_type)s"
            elements = self.rspec.xml.xpath(XPATH_RESOURCE % locals())
            elements = [self.dict_from_elt(network, n.element) for n in elements]

        # XXX if network == self.network == None, we might not have a dict here !!!
        if resource_type in MAP:
            elements = [self.dict_rename(n, resource_type) for n in elements]

        return elements

    def dict_resources(self, network=None):
        """
        \brief Returns a list of resources from the specified network (eventually None)
        """
        result=[]

        # NODES / CHANNELS / LINKS
        for type in RESOURCE_TYPES:
            result.extend(self.parse_element(type, network))

        return result

    def dict_leases(self, resources, network=""):
        """
        \brief Returns a list of leases from the specified network (eventually None)
        """
        result=[]

        # XXX All testbeds that have leases have networks XXX
        XPATH_LEASE = "/RSpec/network[@name='%(network)s']/lease"
        lease_elems = self.rspec.xml.xpath(XPATH_LEASE % locals())

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
                if not 'urn' in match:
                    #print "E: Ignored lease with missing 'resource_urn' key:", filt
                    continue

                rsrc_lease['resource_urn'] = match['resource_urn']
                rsrc_lease['network']      = Xrn(match['resource_urn']).authority[0]
                rsrc_lease['hrn']          = Xrn(match['resource_urn']).hrn
                rsrc_lease['type']         = Xrn(match['resource_urn']).type

                result.append(rsrc_lease)
        return result

    def to_dict(self, version):
        """
        \brief Converts a RSpec to two lists of resources and leases.
        \param version Output of the GetVersion() call holding the hrn of the
        authority. This will be used for testbeds not adding this hrn inside
        the RSpecs.

        This function is the entry point for resource parsing. 
        """
        networks = self.rspec.xml.xpath('/RSpec/network/@name')
        networks = [str(n.element) for n in networks]

        if not networks:
            # NOTE: GENI aggregate for example do not add the network alongside
            # the resources
            networks = []
            # We might retrieve the network from GetVersion() if it is not
            # explicit in the RSpec

            # XXX Jordan: do we really need to store it in self ? I would pass it as a parameter
            # I found the answer: it's because the XPATH expression is different if the RSpec has
            # the network or not
            self.network = version.get('hrn')

            resources = self.dict_resources()
            leases    = self.dict_leases(resources)

        else:
            # NOTE: A resource might have several networks (eg. from a SM)
            for network in networks:
                resources = self.dict_resources(network)
                leases    = self.dict_leases(resources,network)

        return {'resource': resources, 'lease': leases}


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
