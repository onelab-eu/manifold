#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, time, copy, uuid
from types import StringTypes
from manifold.gateways.sfa.rspecs import RSpecParser
from manifold.gateways.sfa.rspecs.sfawrap import SFAWrapParser
import dateutil.parser
from datetime import datetime
from manifold.util.log          import Log
from sfa.rspecs.rspec import RSpec
from sfa.util.xml import XpathFilter
from sfa.util.xrn import Xrn, get_leaf, urn_to_hrn

HEADER = """
<?xml version="1.1" encoding="UTF-8"?> 
"""

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
}

# The key for resources, used for leases
RESOURCE_KEY = 'urn' # 'resource_hrn'

#   RSPEC_ELEMENT
#       rspec_property -> dictionary that is merged when we encounter this
#                         property (the property value is passed as an argument)
#       '*' -> dictionary merged at the end, useful to add some properties made
#              from the combination of several others (the full dictionary is
#              passed as an argument)
HOOKS = {
    'node': {
        'component_id': lambda value : {'hrn': Xrn(value).get_hrn(), 'urn': value,'hostname':  Xrn(value).get_hrn()} # hostname TEMP FIX XXX
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


class OfeliaVTParser(RSpecParser):

    #---------------------------------------------------------------------------
    # RSpec parsing
    #---------------------------------------------------------------------------

    # XXX parse = SFAWrapParser.parse
    @classmethod
    def parse(cls, rspec, rspec_version = None, slice_urn = None):
        Log.warning("OfeliaVT Parser parse slice_urn = ",slice_urn)
        return SFAWrapParser.parse(rspec, rspec_version = None, slice_urn = None)

# DISABLED    @classmethod
# DISABLED    def parse(cls, rspec, rspec_version = None, slice_urn = None):
# DISABLED        Log.warning("OfeliaVT Parser parse slice_urn = ",slice_urn)
# DISABLED        resources   = list()
# DISABLED        leases      = list()
# DISABLED
# DISABLED        rspec = RSpec(rspec)
# DISABLED
# DISABLED        # Parse leases first, so that they can be completed when encountering
# DISABLED        # their ids in resources
# DISABLED        lease_map = dict() # id -> lease_dict
# DISABLED        elements = rspec.xml.xpath('//ol:lease')
# DISABLED 
# DISABLED        # XXX @Loic make network_hrn consistent, Hardcoded !!!      
# DISABLED        network = 'omf.nitos'
# DISABLED
# DISABLED        for el in elements:
# DISABLED            try:
# DISABLED                lease_tmp = cls.dict_from_elt(network, el.element)
# DISABLED                start = time.mktime(dateutil.parser.parse(lease_tmp['valid_from']).utctimetuple())
# DISABLED                end   = time.mktime(dateutil.parser.parse(lease_tmp['valid_until']).utctimetuple())
# DISABLED                lease = {
# DISABLED                    'lease_id': lease_tmp['id'],
# DISABLED                    'slice': slice_urn,
# DISABLED                    'start_time': start,
# DISABLED                    'end_time': end,
# DISABLED                    'duration': (end - start) / cls.get_grain(),
# DISABLED                    'granularity': cls.get_grain()
# DISABLED                }
# DISABLED                lease_map[lease_tmp['id']] = lease
# DISABLED            except:
# DISABLED                import traceback
# DISABLED                Log.warning("this lease has not the right format")
# DISABLED                traceback.print_exc()
# DISABLED
# DISABLED        # Parse nodes
# DISABLED        for tag, resource_type in RESOURCE_TYPES.items():
# DISABLED            if ':' in tag:
# DISABLED                ns, _, tag = tag.partition(':')
# DISABLED                XPATH_RESOURCE = "//%(ns)s:%(tag)s"
# DISABLED            else:
# DISABLED                XPATH_RESOURCE = "//default:%(tag)s | //%(tag)s"
# DISABLED            elements = rspec.xml.xpath(XPATH_RESOURCE % locals()) 
# DISABLED            for el in elements:
# DISABLED                resource = cls.dict_from_elt(network, el.element)
# DISABLED                if resource_type in MAP:
# DISABLED                    resource = cls.dict_rename(resource, resource_type)
# DISABLED                resource['network_hrn'] = network
# DISABLED                resources.append(resource)
# DISABLED
# DISABLED                # Leases
# DISABLED                if 'lease_ref.id_ref' in resource:
# DISABLED                    lease_id_ref = resource.pop('lease_ref.id_ref')
# DISABLED                    lease = copy.deepcopy(lease_map[lease_id_ref])
# DISABLED                    lease['resource'] = resource['urn']
# DISABLED
# DISABLED                    leases.append(lease)
# DISABLED
# DISABLED        return {'resource': resources, 'lease': leases}

    #---------------------------------------------------------------------------
    # RSpec construction
    #---------------------------------------------------------------------------

    @classmethod
    def build_rspec(cls, slice_hrn, resources, leases, flowspace, rspec_version = None):
        Log.warning("OfeliaVTParser Parser build")
        rspec = []
        cls.rspec_add_header(rspec)
        #lease_map = cls.rspec_add_leases(rspec, leases)
        Log.warning(resources)
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
 
    #---------------------------------------------------------------------------
    # RSpec construction helpers
    #---------------------------------------------------------------------------

    @classmethod
    def rspec_add_header(cls, rspec):
        rspec.append(HEADER)
        import time
        h1 = int(time.strftime("%H"))+1
        today = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        today_h1 = time.strftime("%Y-%m-%dT")+h1+time.strftime(":%M:%SZ")
        rspec.append('<RSpec type="SFA" expires="'+today_h1+'" generated="'+today+'">')

    @classmethod
    def rspec_add_footer(cls, rspec):
        rspec.append('</rspec>')

# DISABLED    @classmethod
# DISABLED    def rspec_add_leases(cls, rspec, leases):
# DISABLED        # A map (resource key) -> (lease_id)
# DISABLED        lease_map = {}
# DISABLED
# DISABLED        # A map (interval) -> (lease_id) to group leases by interval
# DISABLED        map_interval_lease_id = {}
# DISABLED
# DISABLED        for lease in leases:
# DISABLED            interval = (lease['start_time'], lease['end_time'])
# DISABLED            if not interval in map_interval_lease_id:
# DISABLED                map_interval_lease_id[interval] = {'client_id': str(uuid.uuid4()), 'lease_id': lease['lease_id']}
# DISABLED            lease_map[lease['resource']] = map_interval_lease_id[interval]
# DISABLED                
# DISABLED        for (valid_from, valid_until), client_id in map_interval_lease_id.items():
# DISABLED            valid_from_iso = datetime.utcfromtimestamp(int(valid_from)).isoformat()
# DISABLED            valid_until_iso = datetime.utcfromtimestamp(int(valid_until)).isoformat()
# DISABLED            if leases.get('lease_id'):
# DISABLED                rspec.append(OLD_LEASE_TAG % locals())
# DISABLED            else:
# DISABLED                rspec.append(NEW_LEASE_TAG % locals())
# DISABLED
# DISABLED        return lease_map

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
        rspec.append(CHANNEL_TAG % channel)
        if lease_id:
            cls.rspec_add_lease_ref(rspec, lease_id)
        rspec.append(CHANNEL_TAG_END)

    @classmethod
    def rspec_add_link(cls, rspec, link, lease_id):
        rspec.append(LINK_TAG % link)
        if lease_id:
            cls.rspec_add_lease_ref(rspec, lease_id)
        rspec.append(LINK_TAG_END)

    @classmethod
    def rspec_add_resources(cls, rspec, resources, lease_map):
        Log.warning(resources)
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

            lease_id_client_id = lease_map.get(resource['urn'])
            lease_id = lease_id_client_id.get('lease_id')
            if not lease_id:
                lease_id = lease_id_client_id.get('client_id')

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

if __name__ == '__main__':
                
    OF_ADVERT     = '/root/ofelia-bristol-of.rspec'
    OF_REQUEST    = '/root/of_request_uob_all.rspec'
    OF_MANIFEST   = '/root/of_manifest.rspec'

    #VTAM_ADVERT   = '/root/VTAM_AD_RSPEC.txt'
    VTAM_ADVERT   = '/root/ofelia-bristol-vt.rspec'
    # '/root/of_request_uob_packet_optical_test2.rspec.xml' ??
    VTAM_REQUEST  = '/root/VTAM_REQUEST_RSPEC.txt'
    VTAM_MANIFEST = None

    TEST_DICT = {
        'controller': 'http://my.controller:1234',
        'flowspaces': [ 
            {
                'matches': [],
                'groups': []
            }, {
                'matches': [],
                'groups': []
            }
         ]
    }

    def test_parse_all():
       
        #for rspec_file in [OF_ADVERT, OF_REQUEST, OF_MANIFEST, VTAM_ADVERT, VTAM_REQUEST, VTAM_MANIFEST]:
        for rspec_file in [VTAM_ADVERT, VTAM_REQUEST, VTAM_MANIFEST]:
            print "file = ",rspec_file
            result = test_parse(rspec_file)
            print "RESULT = "
            print result
            print "========================"

    def test_parse(rspec_file):
        parser = OfeliaVTParser()
        return parser.parse(rspec_file)

    test_parse_all()
