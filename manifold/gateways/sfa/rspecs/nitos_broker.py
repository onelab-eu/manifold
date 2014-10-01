#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, time, copy, uuid
from types import StringTypes
from manifold.gateways.sfa.rspecs import RSpecParser
import dateutil.parser
import calendar
from datetime import datetime, timedelta
from manifold.util.log          import Log
from sfa.rspecs.rspec import RSpec
from sfa.util.xml import XpathFilter
from sfa.util.xrn import Xrn, get_leaf, urn_to_hrn


RESOURCE_TYPES = {
    'node': 'node',
    'link': 'link',
    'ol:channel': 'channel',
}

LIST_ELEMENTS = {
    'node'      : ['id_ref', 'lease_ref.id_ref'],
    'link'      : ['id_ref', 'lease_ref.id_ref'],
    'channel'   : ['id_ref', 'lease_ref.id_ref']
}

GRANULARITY = 1800
NEW_LEASE_TAG = '<ol:lease client_id="%(client_id)s" valid_from="%(valid_from_iso)sZ" valid_until="%(valid_until_iso)sZ"/>'
OLD_LEASE_TAG = '<ol:lease id="%(lease_id)s" valid_from="%(valid_from_iso)sZ" valid_until="%(valid_until_iso)sZ"/>'
#NEW_LEASE_TAG = '<ol:lease client_id="%(client_id)s" valid_from="%(valid_from_iso)s" valid_until="%(valid_until_iso)s"/>'
#OLD_LEASE_TAG = '<ol:lease id="%(lease_id)s" valid_from="%(valid_from_iso)s" valid_until="%(valid_until_iso)s"/>'
LEASE_REF_TAG = '<ol:lease_ref id_ref="%(lease_id)s"/>'
NODE_TAG = '<node component_id="%(urn)s">' # component_manager_id="urn:publicid:IDN+omf:xxx+authority+am" component_name="node1" exclusive="true" client_id="my_node">'
NODE_TAG_END = '</node>'
CHANNEL_TAG = '<ol:channel component_id="%(urn)s">'
CHANNEL_TAG_END = '</ol:channel>'

# XXX Not tested:
LINK_TAG = '<link component_id="%(urn)s">'
LINK_TAG_END = '</link>'




class NITOSBrokerParser(RSpecParser):

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
    @staticmethod
    def channel_urn_hrn_exclusive(value):
        output = {}
        # XXX HARDCODED FOR NITOS
        xrn = Xrn('%(network)s.%(component_name)s' % value, type='channel')
        return {'urn': xrn.urn, 'hrn': xrn.hrn, 'exclusive': True, 'hostname': xrn.hrn} # hostname TEMP FIX XXX
        return {'exclusive': True, 'hostname': xrn.hrn} # hostname TEMP FIX XXX

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
            '*': lambda value: NITOSBrokerParser.channel_urn_hrn_exclusive(value)
        },
        '*': {
            'exclusive': lambda value: {'exclusive': value.lower() not in ['false']}
        }
    }

    # END HOOKS

    #---------------------------------------------------------------------------
    # RSpec parsing
    #---------------------------------------------------------------------------

    @classmethod
    def parse(cls, rspec, rspec_version = None, slice_urn = None):

        resources   = list()
        leases      = list()

        rspec = RSpec(rspec)

        # Parse leases first, so that they can be completed when encountering
        # their ids in resources
        lease_map = dict() # id -> lease_dict
        elements = rspec.xml.xpath('//ol:lease')
 
        # XXX @Loic make network_hrn consistent, Hardcoded !!!      
        network = 'omf.nitos'

        for el in elements:
            try:
                lease_tmp = cls.dict_from_elt(network, el.element)
                start = calendar.timegm(dateutil.parser.parse(lease_tmp['valid_from']).utctimetuple())
                end   = calendar.timegm(dateutil.parser.parse(lease_tmp['valid_until']).utctimetuple())
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
                resource = cls.dict_from_elt(network, el.element, LIST_ELEMENTS.get(resource_type))
                if resource_type in cls.MAP:
                    resource = cls.dict_rename(resource, resource_type)
                resource['network_hrn'] = network
                resource['facility_name'] = 'NITOS'
                resource['testbed_name'] = 'NITOS'
                resources.append(resource)

                # Leases
                if 'lease_ref.id_ref' in resource:
                    lease_id_refs = resource.pop('lease_ref.id_ref')
                    for lease_id_ref in lease_id_refs:
                        lease = copy.deepcopy(lease_map[lease_id_ref])
                        lease['resource'] = resource['urn']

                        leases.append(lease)

        return {'resource': resources, 'lease': leases}

    #---------------------------------------------------------------------------
    # RSpec construction
    #---------------------------------------------------------------------------

    @classmethod
    def build_rspec(cls, slice_hrn, resources, leases, flowspace, rspec_version = None):
        Log.warning("NitosBroker Parser build")
        rspec = []
        cls.rspec_add_header(rspec)
        lease_map = cls.rspec_add_leases(rspec, leases)
        cls.rspec_add_resources(rspec, resources, lease_map)
        cls.rspec_add_footer(rspec)
        return "\n".join(rspec)

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
        # A map (resource key) -> [ { client_id: UUID, lease_id: ID_or_None }, ... ]
        lease_map = {}

        # A map (interval) -> (lease_id) to group reservations by interval == 1 RSPEC LEASE
        map_interval_lease_id = {}

        for lease in leases:
            interval = (lease['start_time'], lease['end_time'])
            if not interval in map_interval_lease_id:
                map_interval_lease_id[interval] = {'lease_id': None, 'client_id': str(uuid.uuid4()) }
            lease_id = lease.get('lease_id')
            if lease_id:
                # If grouped, all leases will have the same ID, so we can update it each time
                map_interval_lease_id[interval]['lease_id'] = lease_id

            if not lease['resource'] in lease_map:
                lease_map[lease['resource']] = list()
            lease_map[lease['resource']].append(map_interval_lease_id[interval])
                
        for (valid_from, valid_until), lease_dict in map_interval_lease_id.items():
            valid_from_iso = datetime.utcfromtimestamp(int(valid_from)).isoformat()
            valid_until_iso = datetime.utcfromtimestamp(int(valid_until)).isoformat()

            # NITOS Broker not supporting timezones
            #valid_from_iso = "%s%+02d:%02d" %  ((datetime.utcfromtimestamp(int(valid_from))  + timedelta(hours=3)).isoformat(), 3, 00)
            #valid_until_iso = "%s%+02d:%02d" % ((datetime.utcfromtimestamp(int(valid_until)) + timedelta(hours=3)).isoformat(), 3, 00)

            lease_id = lease_dict['lease_id']
            client_id = lease_dict['client_id']

            if lease_id:
                rspec.append(OLD_LEASE_TAG % locals())
            else:
                rspec.append(NEW_LEASE_TAG % locals())

        return lease_map

    @classmethod
    def rspec_add_lease_ref(cls, rspec, lease_id):
        if lease_id:
            rspec.append(LEASE_REF_TAG % locals())

    @classmethod
    def rspec_add_node(cls, rspec, node, lease_ids):
        rspec.append(NODE_TAG % node)
        for lease_id in lease_ids:
            cls.rspec_add_lease_ref(rspec, lease_id)
        rspec.append(NODE_TAG_END)

    @classmethod
    def rspec_add_channel(cls, rspec, channel, lease_ids):
        rspec.append(CHANNEL_TAG % channel)
        for lease_id in lease_ids:
            cls.rspec_add_lease_ref(rspec, lease_id)
        rspec.append(CHANNEL_TAG_END)

    @classmethod
    def rspec_add_link(cls, rspec, link, lease_ids):
        rspec.append(LINK_TAG % link)
        for lease_id in lease_ids:
            cls.rspec_add_lease_ref(rspec, lease_id)
        rspec.append(LINK_TAG_END)

    @classmethod
    def rspec_add_resources(cls, rspec, resources, lease_map):
        for resource in resources:
            if isinstance(resource, StringTypes):
                urn = resource
                hrn, type = urn_to_hrn(urn)

                resource = {
                    'urn'           : urn,
                    'hrn'           : hrn,
                    'type'          : type,
                }
            # What information do we need in resources for REQUEST ?
            resource_type = resource.pop('type')

            # We add lease_ref wrt to each lease_id (old leases) and each client_id (new leases)
            lease_dicts = lease_map.get(resource['urn'])

            # NOTE : Shall we ignore reservation of resources without leases ?
            lease_ids = list()
            if lease_dicts:
                for lease_dict in lease_dicts:
                    lease_id = lease_dict.get('lease_id')
                    if not lease_id:
                        lease_id = lease_dict.get('client_id')
                    lease_ids.append(lease_id)

            if resource_type == 'node':
                cls.rspec_add_node(rspec, resource, lease_ids)
            elif resource_type == 'link':
                cls.rspec_add_link(rspec, resource, lease_ids)
            elif resource_type == 'channel':
                cls.rspec_add_channel(rspec, resource, lease_ids)

    @classmethod
    def get_grain(cls):
        return 1800

    @classmethod
    def get_min_duration(cls):
        return 1800
                

#rspec = RSpec(open(sys.argv[1]).read())
#NITOSBrokerParser.parse(rspec)
