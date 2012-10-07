from types import StringTypes 
from sfa.rspecs.rspec import RSpec
from sfa.util.xrn import Xrn, urn_to_hrn
from tophat.gateways.sfa.rspecs import RSpecParser
from tophat.core.filter import Filter
from sfa.util.xml import XpathFilter

from sfa.rspecs.elements.versions.nitosv1Lease import NITOSv1Lease
from sfa.rspecs.elements.versions.nitosv1Channel import NITOSv1Channel

MAP = {
    'node': {
        'location.country': 'country',
        'position_3d.x': 'x',
        'position_3d.y': 'y',
        'position_3d.z': 'z',
        'granularity.grain': 'granularity', # harmonize between ple and nitos
    },
    'spectrum/channel': {
    }
}

# HOOKS TO RUN OPERATIONS ON GIVEN FIELDS
def channel_urn_hrn(value):
    output = {}
    urn = 'urn:publicid:IDN+%s:channel+%s' % (value['network'], value['channel_num'])
    hrn = Xrn(urn).get_hrn()
    print "I: Forging channel urn... not sure of the syntax"
    return {'urn': urn, 'hrn': hrn}

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
                    leases.append(rsrc_lease)
        print ""
        print ""
        print "leases:"
        print "======="
        for l in leases:
            print l
        print "======="
        return {'resource': resources, 'lease': leases}





# Former parsing code

#            #print node_elems
#            nodes = []
#
#            for n in nodes:
#                # NODE {
#                #    'exclusive': None,
#                #    'tags': [{'tagname': 'hostname', 'value': 'ple5.ipv6.lip6.fr'}, {'tagname': 'interface', 'value': None}, {'tagname': 'pldistro', 'value': 'onelab'}, {'tagname': 'arch', 'value': 'x86_64'}, {'tagname': 'fcdistro', 'value': 'f14'}, {'tagname': 'bww', 'value': '183.3'}, {'tagname': 'astype', 'value': 'n/a'}, {'tagname': 'reliability', 'value': '100'}, {'tagname': 'load', 'value': '0.6'}, {'tagname': 'slices', 'value': '1'}, {'tagname': 'cpu', 'value': '0'}, {'tagname': 'mem', 'value': '57'}, {'tagname': 'country', 'value': 'France'}, {'tagname': 'bw', 'value': '76.3'}, {'tagname': 'reliabilityy', 'value': '33'}, {'tagname': 'response', 'value': '15.1'}, {'tagname': 'loady', 'value': '0.7'}, {'tagname': 'slicesy', 'value': '0'}, {'tagname': 'bwm', 'value': '42.8'}, {'tagname': 'memm', 'value': '11'}, {'tagname': 'loadm', 'value': '0.6'}, {'tagname': 'asnumber', 'value': '1307'}, {'tagname': 'cpum', 'value': '0'}, {'tagname': 'responsem', 'value': '2.3'}, {'tagname': 'reliabilitym', 'value': '16'}, {'tagname': 'city', 'value': 'Paris'}, {'tagname': 'region', 'value': 'Ile-de-France'}, {'tagname': 'bwy', 'value': '236.7'}, {'tagname': 'memy', 'value': '13'}, {'tagname': 'cpuy', 'value': '29'}, {'tagname': 'slicesm', 'value': '0'}, {'tagname': 'loadw', 'value': '0.6'}, {'tagname': 'slicesw', 'value': '0'}, {'tagname': 'cpuw', 'value': '0'}, {'tagname': 'memw', 'value': '47'}, {'tagname': 'responsew', 'value': '10.0'}, {'tagname': 'hrn', 'value': 'planetlab.test.upmc.ple5'}, {'tagname': 'reliabilityw', 'value': '71'}, {'tagname': 'responsey', 'value': '1.3'}, {'tagname': 'sliver', 'value': None}],
#                #     'boot_state': None,
#                #     'interfaces': [{'component_id': 'urn:publicid:IDN+ple+interface+node14281:eth0', 'interface_id': None, 'node_id': None, 'role': None, 'ipv4': '132.227.62.123', 'client_id': '14281:168', 'mac_address': None, 'bwlimit': None}],
#                #     'site_id': 'urn:publicid:IDN+ple:upmc+authority+sa',
#                #     'authority_id': 'urn:publicid:IDN+ple:upmc+authority+sa', 
#                #     'hardware_types': [],
#                #     'disk_images': None,
#                #     'pl_initscripts': None,
#                #     'client_id': None,
#                #     'services': [{'execute': [], 'login': [], 'install': []}],
#                #     'component_manager_id': 'urn:publicid:IDN+ple+authority+cm',
#                #     'slivers': [{'disk_images': None, 'sliver_id': None, 'component_id': 'urn:publicid:IDN+ple:upmc+node+ple5.ipv6.lip6.fr', 'name': 'upmc_agent', 'client_id': None, 'tags': [], 'type': None}],
#                #     'component_id': 'urn:publicid:IDN+ple:upmc+node+ple5.ipv6.lip6.fr', 
#                #     'bw_limit': None,
#                #     'bw_unallocated': None,
#                #     'sliver_id': None,
#                #     'location': {'latitude': '48.8525', 'country': 'unknown', 'longitude': '2.27849'},
#                #     'component_name': 'ple5.ipv6.lip6.fr'
#                #}
#                node = {
#                    #'type': ['resource', 'node']
#                    'type': 'node'
#                }
#                for k,v in n.items():
#                    if not v or isinstance(v, StringTypes):
#                        node[k] = v
#                    elif isinstance(v, dict):
#                        for x,y in v.items():
#                            node[x] = y
#                            #node[k+'.'+x] = y
#                    elif k == 'tags':
#                        for t in v:
#                            node[t['tagname']] = t['value']
#                    elif isinstance(v, list):
#                        node[k] = []
#                        for elt in v:
#                            if isinstance(elt, dict):
#                                node[k].append(dict(elt))
#                            else:
#                                node[k].append(elt)
#                                
#                    else:
#                        print "Unknown type in SFAv1Parser::to_dict()"
#                        print v
#                        import sys
#                        sys.exit(1)
#
#                if 'component_manager_id' in node and node['component_manager_id']:
#                    node['network'] = urn_to_hrn(node['component_manager_id'])[0]
#                elif 'component_id' in node and node['component_id']:
#                    node['network'] = Xrn.hrn_split(Xrn(node['component_id']).get_hrn())[0]
#                    node['hrn'] = Xrn(node['component_id']).get_hrn()
#                elif "hostname" in node and 'gpeni' in node['hostname']: # heuristic ?
#                    node['network'] = 'plc.gpeni'
#                output.append(node)
#
#            # BUG channels = NITOSv1Channel.get_channels(self.rspec.xml, filter=None)
#            channel_elems = self.rspec.xml.xpath('//spectrum/channel')
#
#            for channel in channel_elems:
#                rsrc = {'type': 'channel'}
#                rsrc.update(channel.attrib)
#
#
#            #links = rspec2.version.xml.xpath('//network[@name="%s"]/link' % network)
#            links = self.rspec.get_links() # How to filter nodes from a given network
#            for link in links:
#                # LINK {
#                #    'latency': None, 'component_id': None, 'capacity': None, 'packet_loss': None, 'interface1': None, 'interface2': None, 'endpoints': 'ksu ku', 'component_manager': None, 'client_id': None, 'component_name': None, 'type': None, 'description': None}  
#                rsrc = {
#                    #'type': ['resource', 'link']
#                    'type': 'link'
#                }
#                try:
#                    rsrc['sites'] = tuple(link.attrib['endpoints'].split(' '))
#                except:
#                    pass#print 'no sites in link'
#                for x in link.iterchildren():
#                    if x.text:
#                        rsrc[x.tag] = x.text
#                        # description: plc.gpeni
#                        # bw_unallocated ( XXX units): plc.gpeni
#                    else:
#                        for k,v in x.attrib.items():
#                            rsrc[k] = v
#                            # location (latitude, country, longitude): plc.gpeni
#                if 'component_manager_id' in rsrc and rsrc['component_manager_id']:
#                    rsrc['network'] = urn_to_hrn(rsrc['component_manager_id'])[0]
#                elif 'component_id' in rsrc and rsrc['component_id']:
#                    rsrc['network'] = Xrn.hrn_split(Xrn(rsrc['component_id']).get_hrn())[0]
#                elif 'description' in rsrc and ' -- ' in rsrc['description']: # heuristic ?
#                    rsrc['network'] = 'plc.gpeni'
#                output.append(rsrc)
#            # MAXPL: computeResource/computeNode/networkInterface TODO
#            return output
