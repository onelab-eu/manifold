#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, time, copy, uuid
from types import StringTypes
from manifold.gateways.sfa.rspecs import RSpecParser
import dateutil.parser
from datetime import datetime
from manifold.util.log          import Log
from sfa.rspecs.rspec import RSpec
from sfa.util.xml import XpathFilter
from sfa.util.xrn import Xrn, get_leaf, urn_to_hrn


RESOURCE_TYPES = {
    'node': 'node',
    'link': 'link',
    'ol:channel': 'channel',
}

GRANULARITY = 1800
LEASE_TAG = '<ol:lease client_id="%(client_id)s" valid_from="%(valid_from_iso)sZ" valid_until="%(valid_until_iso)sZ"/>'
LEASE_REF_TAG = '<ol:lease_ref id_ref="%(lease_id)s"/>'
NODE_TAG = '<node component_id="%(urn)s">' # component_manager_id="urn:publicid:IDN+omf:xxx+authority+am" component_name="node1" exclusive="true" client_id="my_node">'

NODE_TAG_END = '</node>'

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
    'channel': {
    }
}

# The key for resources, used for leases
RESOURCE_KEY = 'urn' # 'resource_hrn'

# HOOKS TO RUN OPERATIONS ON GIVEN FIELDS
def channel_urn_hrn_exclusive(value):
    output = {}
    # XXX HARDCODED FOR NITOS
    xrn = Xrn('%(network)s.nitos.channel.%(component_name)s' % value, type='channel')
    return {'urn': xrn.urn, 'hrn': xrn.hrn, 'exclusive': True}

#   RSPEC_ELEMENT
#       rspec_property -> dictionary that is merged when we encounter this
#                         property (the property value is passed as an argument)
#       '*' -> dictionary merged at the end, useful to add some properties made
#              from the combination of several others (the full dictionary is
#              passed as an argument)
HOOKS = {
    'node': {
        'component_id': lambda value : {'hrn': Xrn(value).get_hrn(), 'urn': value}
    },
    'link': {
        'component_id': lambda value : {'hrn': Xrn(value).get_hrn(), 'urn': value}
    },
    'channel': {
        '*': lambda value: channel_urn_hrn_exclusive(value)
    },
    '*': {
        'exclusive': lambda value: {'exclusive': value.lower() not in ['false']}
    }
}

# END HOOKS


class NITOSBrokerParser(RSpecParser):

    #---------------------------------------------------------------------------
    # RSpec parsing
    #---------------------------------------------------------------------------

    @classmethod
    def parse(cls, rspec, slice_urn = None):
        Log.warning("NitosBroker Parser parse")

        resources   = list()
        leases      = list()

        rspec = RSpec(rspec)

        # Parse leases first, so that they can be completed when encountering
        # their ids in resources
        lease_map = dict() # id -> lease_dict
        elements = rspec.xml.xpath('//ol:lease')
 
        # XXX @Loic make network_hrn consistent, Hardcoded !!!      
        network = 'nitos'

        for el in elements:
            try:
                lease_tmp = cls.dict_from_elt(network, el.element)
                start = time.mktime(dateutil.parser.parse(lease_tmp['valid_from']).timetuple())
                end   = time.mktime(dateutil.parser.parse(lease_tmp['valid_until']).timetuple())
                lease = {
                    'lease_id': lease_tmp['id'],
                    'slice': slice_urn,
                    'start_time': start,
                    'end_time': end,
                    'duration': (end - start) / cls.get_grain(),
                    'granularity': cls.get_grain()
                }
                lease_map[lease_tmp['id']] = lease
            except:
                import traceback
                Log.warning("this lease has not the right format")
                traceback.print_exc()

        # Parse nodes
        for tag, resource_type in RESOURCE_TYPES.items():
            if ':' in tag:
                ns, _, tag = tag.partition(':')
                XPATH_RESOURCE = "//%(ns)s:%(tag)s"
            else:
                XPATH_RESOURCE = "//default:%(tag)s | //%(tag)s"
            elements = rspec.xml.xpath(XPATH_RESOURCE % locals()) 
            for el in elements:
                resource = cls.dict_from_elt(network, el.element)
                if resource_type in MAP:
                    resource = cls.dict_rename(resource, resource_type)
                resource['network_hrn'] = network
                resources.append(resource)

                # Leases
                if 'lease_ref.id_ref' in resource:
                    lease_id_ref = resource.pop('lease_ref.id_ref')
                    lease = copy.deepcopy(lease_map[lease_id_ref])
                    lease['resource'] = resource['urn']

                    leases.append(lease)

        return {'resource': resources, 'lease': leases}

    #---------------------------------------------------------------------------
    # RSpec construction
    #---------------------------------------------------------------------------

    @classmethod
    def build_rspec(cls, slice_hrn, resources, leases, rspec_version = None):
        Log.warning("NitosBroker Parser build")
        rspec = []
        cls.rspec_add_header(rspec)
        lease_map = cls.rspec_add_leases(rspec, leases)
        cls.rspec_add_resources(rspec, resources, lease_map)
        cls.rspec_add_footer(rspec)
        return "\n".join(rspec)

    #---------------------------------------------------------------------------
    # RSpec parsing helpers
    #---------------------------------------------------------------------------

    @classmethod
    def get_element_tag(self, element):
        tag = element.tag
        if element.prefix in element.nsmap:
            # NOTE: None is a prefix that can be in the map (default ns)
            start = len(element.nsmap[element.prefix]) + 2 # {ns}tag
            tag = tag[start:]
        
        return tag

    @classmethod
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
 
    @classmethod
    def dict_from_elt(self, network, element):
        """
        Returns an object
        """
        ret            = {}
        ret['network'] = network
        ret['type']    = self.get_element_tag(element)
 
        for k, v in element.attrib.items():
            ret[k] = v
 
        for c in element.getchildren():
            ret.update(self.prop_from_elt(c))
 
        return ret
 
    @classmethod
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
 
    @classmethod
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
 
    @classmethod
    def dict_resources(self, network=None):
        """
        \brief Returns a list of resources from the specified network (eventually None)
        """
        result=[]
 
        # NODES / CHANNELS / LINKS
        for type in RESOURCE_TYPES:
            result.extend(self.parse_element(type, network))
 
        return result
 
    @classmethod
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
                    print "W: Granularity not present in node:", filt
                    pass
                else:
                    rsrc_lease['granularity'] = match['granularity']
                if not 'urn' in match:
                    #print "E: Ignored lease with missing 'resource_urn' key:", filt
                    continue
 
                rsrc_lease['urn']          = match['urn']
                rsrc_lease['network_hrn']  = Xrn(match['urn']).authority[0]
                rsrc_lease['hrn']          = Xrn(match['urn']).hrn
                rsrc_lease['type']         = Xrn(match['urn']).type
 
                result.append(rsrc_lease)
        return result
 
    @classmethod
    def to_dict(self, version):
        """
        \brief Converts a RSpec to two lists of resources and leases.
        \param version Output of the GetVersion() call holding the hrn of the
        authority. This will be used for testbeds not adding this hrn inside
        the RSpecs.
 
        This function is the entry point for resource parsing. 
        """
        Log.tmp("TO DICT")
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

    @classmethod
    def rspec_add_header(cls, rspec):
        rspec.append("""<?xml version="1.0"?>
<rspec type="request" xmlns="http://www.geni.net/resources/rspec/3" xmlns:ol="http://nitlab.inf.uth.gr/schema/sfa/rspec/1" xmlns:omf="http://schema.mytestbed.net/sfa/rspec/1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.geni.net/resources/rspec/3 http://www.geni.net/resources/rspec/3/request.xsd http://nitlab.inf.uth.gr/schema/sfa/rspec/1 http://nitlab.inf.uth.gr/schema/sfa/rspec/1/request-reservation.xsd">""")

    @classmethod
    def rspec_add_footer(cls, rspec):
        rspec.append('</rspec>')

    @classmethod
    def rspec_add_leases(cls, rspec, leases):
        # A map (resource key) -> (lease_id)
        lease_map = {}

        # A map (interval) -> (lease_id) to group leases by interval
        map_interval_lease_id = {}

        for lease in leases:
            interval = (lease['start_time'], lease['end_time'])
            if not interval in map_interval_lease_id:
                map_interval_lease_id[interval] = str(uuid.uuid4())
            lease_map[lease['resource']] = map_interval_lease_id[interval]
                
        for (valid_from, valid_until), client_id in map_interval_lease_id.items():
            valid_from_iso = datetime.fromtimestamp(int(valid_from)).isoformat()
            valid_until_iso = datetime.fromtimestamp(int(valid_until)).isoformat()
            rspec.append(LEASE_TAG % locals())

        return lease_map

    @classmethod
    def rspec_add_lease_ref(cls, rspec, lease_id):
        if lease_id:
            rspec.append(LEASE_REF_TAG % locals())

    @classmethod
    def rspec_add_node(cls, rspec, node, lease_id):
        rspec.append(NODE_TAG % node)
        if lease_id:
            cls.rspec_add_lease_ref(rspec, lease_id)
        rspec.append(NODE_TAG_END)

    @classmethod
    def rspec_add_channel(cls, rspec, channel, lease_id):
        pass

    @classmethod
    def rspec_add_link(cls, rspec, link, lease_id):
        pass

    @classmethod
    def rspec_add_resources(cls, rspec, resources, lease_map):
        for resource in resources:
            if isinstance(resource, StringTypes):
                urn = resource
                hrn, type = urn_to_hrn(urn)

                resource = {
                    'urn': urn,
                    'hrn': hrn,
                    'type': type,
                }
            # What information do we need in resources for REQUEST ?
            resource_type = resource.pop('type')
            lease_id = lease_map.get(resource['urn'])
            if resource_type == 'node':
                cls.rspec_add_node(rspec, resource, lease_id)
            elif resource_type == 'link':
                cls.rspec_add_link(rspec, resource, lease_id)
            elif resource_type == 'channel':
                cls.rspec_add_channel(rspec, resource, lease_id)

    @classmethod
    def get_grain(cls):
        return 1800

    @classmethod
    def get_min_duration(cls):
        return 1800
                

#rspec = RSpec(open(sys.argv[1]).read())
#NITOSBrokerParser.parse(rspec)
