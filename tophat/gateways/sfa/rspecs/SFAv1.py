from types import StringTypes 
from sfa.rspecs.rspec import RSpec
from sfa.util.xrn import Xrn, get_leaf
from tophat.gateways.sfa.rspecs import RSpecParser
from tophat.core.filter import Filter
from sfa.util.xml import XpathFilter

MAP = {
    'node': {
        'location.country': 'country',
        'location.latitude': 'latitude',
        'location.longitude': 'longitude',
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
def channel_urn_hrn(value):
    output = {}
    xrn = Xrn('%(network)s.%(channel_num)s' % value, type='channel')
    return {'urn': xrn.urn, 'hrn': xrn.hrn}

HOOKS = {
    'node': {
        'component_id': lambda value : {'hrn': Xrn(value).get_hrn(), 'urn': value}
    },
    'spectrum/channel': {
        '*': lambda value: channel_urn_hrn(value)
    }
}

# END HOOKS

class SFAv1Parser(RSpecParser):

    def __init__(self, rspec):
        self.rspec = RSpec(rspec).version

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
        if name in HOOKS and '*' in HOOKS[name]:
            ret.update(HOOKS[name]['*'](ret))
        return ret

    def parse_element(self, network, name):
        elements = self.rspec.xml.xpath("/RSpec/network[@name='%s']/%s" % (network, name))
        elements = [self.dict_from_elt(network, n.element) for n in elements]
        if name in MAP:
            elements = [self.dict_rename(n, name) for n in elements]
        return elements

    def to_dict(self):

        networks = self.rspec.xml.xpath('/RSpec/network/@name')
        networks = [str(n.element) for n in networks]

        resources = []
        leases = []

        for network in networks:
            # NODES / CHANNELS / LINKS
            for type in ['node', 'spectrum/channel', 'link']:
                resources.extend(self.parse_element(network, type))

            # LEASES
            lease_elems = self.rspec.xml.xpath("/RSpec/network[@name='%s']/lease" % network)
            for l in lease_elems:
                lease = dict(l.element.attrib)
                for resource_elem in l.element.getchildren():
                    rsrc_lease = lease.copy()
                    filt = self.dict_from_elt(network, resource_elem)
                    match = Filter.from_dict(filt).filter(resources)
                    if len(match) == 0:
                        print "E: Ignored lease with no match:", filt
                        continue
                    if len(match) > 1:
                        print "E: Ignored lease with multiple matches:", filt
                        continue
                    match = match[0]

                    # Check whether the node is reservable
                    # Check whether the node has some granularity
                    if not 'exclusive' in match:
                        print "W: No information about reservation capabilities of the node:", filt
                    else:
                        if not match['exclusive']:
                            print "W: lease on a non-reservable node:", filt
                    if not 'granularity' in match:
                        print "W: Granularity not present in node:", filt
                    else:
                        rsrc_lease['granularity'] = match['granularity']
                    if not 'urn' in match:
                        print "E: Ignored lease with missing 'urn' key:", filt
                        continue
                    rsrc_lease['urn'] = match['urn']
                    rsrc_lease['network'] = Xrn(match['urn']).authority[0]
                    rsrc_lease['hrn'] = Xrn(match['urn']).hrn
                    rsrc_lease['type'] = Xrn(match['urn']).type
                    leases.append(rsrc_lease)
        #print ""
        #print ""
        #print "leases:"
        #print "======="
        for l in leases:
            print l
        #print "======="
        return {'resource': resources, 'lease': leases}

    def rspec_add_header(self, rspec):
        rspec.append('<?xml version="1.0"?>')
        rspec.append('<RSpec type="SFA" expires="2012-10-07T02:24:53Z" generated="2012-10-07T01:24:53Z">')

    def rspec_add_footer(self, rspec):
        rspec.append('</RSpec>')

    def rspec_add_networks(self, rspec, resources_by_network, leases_by_network):
        networks = set(resources_by_network.keys()).union(set(leases_by_network.keys()))
        for n in networks:
            r = resources_by_network[n] if n in resources_by_network else []
            l = leases_by_network[n] if n in leases_by_network else []
            self.rspec_add_network(rspec, network, r, l)

    def rspec_add_network(self, rspec, network, resources, leases):
        rspec.append('  <network name="%s">' % network)
        for r in resources:
            rspec.append('    <node component_id="%s">' % urn)
            rspec.append('      <sliver/>')
            rspec.append('    </node>')
        for l in leases: # Do we need to group ?
            rspec.append('    <lease slice_id="%(slice_id)s" start_time="%(start_time)s" duration="%(duration)s">' % l)
            type = Xrn(l['urn']).type
            val = l['urn'] if isinstance(l, dict) else l
            if type == 'node':
                rspec.append('    <node component_id="%s">' % val)
            elif type == 'channel':
                rspec.append('    <channel channel_num="%s">' % get_leaf(val))
            else:
                print "W: Ignore element while building rspec"
                continue 
            rspec.append('    </lease>')

    def to_rspec(self, resources, leases):
        resources_by_network = {}
        for r in resources:
            network = Xrn(r['urn']).authority[0]
            resources_by_network[network] = r

        leases_by_network = {}
        for l in leases:
            network = Xrn(l['urn']).authority[0]
            leases_by_network[network] = l
            
        rspec = []
        self.rspec_add_header()
        self.rspec_add_networks(resources_by_network, leases_by_network)
        self.rspec_add_footer()
        
        return "\n".join(rspec)
        pass
