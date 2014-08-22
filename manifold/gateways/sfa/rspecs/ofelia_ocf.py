#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid
from datetime                       import datetime
from types                          import StringTypes

from manifold.util.log              import Log
from manifold.gateways.sfa.rspecs   import RSpecParser, RENAME, HOOKS

class OfeliaOcfParser(RSpecParser):

    #---------------------------------------------------------------------------
    # RSpec parsing
    #---------------------------------------------------------------------------

    # Might be confusing for reverse mapping in RSpec construction
    __namespace_map__ = {
        'openflow': None
    }

    __actions__ = {
        'rspec/datapath': {
            RENAME  : {
                '@dpid': 'datapath_id',
                '@component_id': 'component_id',
                '@component_manager_id': 'component_id',
            },
            HOOKS   : [RSpecParser.xrn_hook, RSpecParser.type_hook('datapath')]
            # REMOVE_@
            # REMOVE_#
        },
        'rspec/datapath/port': {
            RENAME  : {
                '@num': 'num',
                '@name': 'name',
            },
        },
        'rspec/link': {
            HOOKS    : [RSpecParser.xrn_hook, RSpecParser.type_hook('link')]
        },
        'rspec/link/source_datapath/datapath': {
            RENAME  : {
                '@dpid': 'datapath_id',
                '@component_id': 'component_id',
                '@component_manager_id': 'component_id',
            },
        },
        'rspec/link/source_datapath/port': {
            RENAME  : {
                '@port_num': 'port',
            },
        },
        'rspec/link/source_datapath/wavelength': {
            RENAME  : {
                '@value': 'value',
            },
        },
        'rspec/link/destination_datapath/datapath': {
            RENAME  : {
                '@dpid': 'datapath_id',
                '@component_id': 'component_id',
                '@component_manager_id': 'component_id',
            },
        },
        'rspec/link/destination_datapath/port': {
            RENAME  : {
                '@port_num': 'num',
            },
        },
        'rspec/link/destination_datapath/wavelength': {
            RENAME  : {
                '@value': 'value',
            },
        },
    }

    @classmethod
    def parse_impl(cls, rspec_dict):
        resources = list()
        leases    = list()

        rspec = rspec_dict.get('rspec')

        datapaths = rspec.get('datapath', list())
        links     = rspec.get('link', list())

        resources.extend(datapaths)
        resources.extend(links)

        return resources, leases

    #---------------------------------------------------------------------------
    # RSpec construction
    #---------------------------------------------------------------------------

    __rspec_request_base_dict__ = {
        u'rspec': {
            u'@xmlns': u'http://www.geni.net/resources/rspec/3',
            u'@xmlns:xs': u'http://www.w3.org/2001/XMLSchema-instance',
            u'@xmlns:openflow': u'/opt/ofelia/ofam/local/schemas',
            u'@xs:schemaLocation': u'http://www.geni.net/resources/rspec/3 http://www.geni.net/resources/rspec/3/request.xsd http://www.geni.net/resources/rspec/ext/openflow/3 http://www.geni.net/resources/rspec/ext/openflow/3/of-resv.xsd',
            u'@type': u'request', 
        }
    }

    @classmethod
    def build_rspec_impl(cls, slice_hrn, resources, leases):
        """
        Returns a dict used to build the request RSpec with xmltodict.

        NOTE: since ad/request/manifest are all different, it is difficult to
        automate translation between Manifold and RSpecs.
        """
        rspec_dict = cls.__rspec_request_base_dict__.copy()
        # Groups
        groups = list()

        # We are expect only 1 Ofelia resource in the Query
        for resource in resources:
            if 'groups' in resource:
                for group in resource['groups']:
                    # in group['ports'], we have a list of dict with datapath and port
                    # properties. We need a listof ports groupes by datapaths in the
                    # rspec.
                    ports_by_datapath = dict()
                    for port in group['ports']:
                        _datapath = port['datapath']
                        _port = port['port']
                        if _datapath not in ports_by_datapath:
                            ports_by_datapath[_datapath] = list()
                        ports_by_datapath[_datapath].append(_port)

                    # Datapaths
                    datapaths = list()
                    for _datapath, _ports in ports_by_datapath.items():

                        # Ports for this datapath
                        ports = list()
                        for _port in _ports:
                            port_dict = {
                                '@num': _port,          # Example: '11'
                                #'@name': None          # Example: 'GBE0/24'
                            }
                            ports.append(port_dict)

                        datapath_dict = {
                            # u'@component_manager_id': u'urn:publicid:IDN+openflow:ofam:univbris+authority+cm', 
                            u'@component_id': _datapath, # Example: u'urn:publicid:IDN+openflow:ofam:univbris+datapath+00:00:00:00:0c:21:00:0a',
                            #u'@dpid': u'00:00:00:00:0c:21:00:0a', 
                            u'openflow:port': ports,
                        }
                        datapaths.append(datapath_dict)

                    group_dict = {
                        '@name': group['name'],
                        'openflow:datapath': datapaths,
                    }
                    groups.append(group_dict)

                # MATCH
                matches = list()
                for match in resource['matches']:
                    # Groups
                    groups = list()
                    for group in match['groups']:
                        group_dict = {
                            '@name': group,
                        }
                        groups.append(group_dict)
                    
                    # Packet description
                    packet_dict = dict()
                    for k, v in match['packet'].items():
                        packet_dict['openflow:'+k] = {'@value': v}
                    match_dict = {
                        'openflow:use-group': groups,
                        'openflow:packet': packet_dict,
                    }
                    matches.append(match_dict)

                sliver = {
                    '@email': 'support@myslice.info',       # XXX used ?
                    '@description': 'TBD',                  # XXX used ?
                    'openflow:controller': {
                        '@url': resource['controller'],    # Example: 'tcp:10.216.22.51:6633'
                        '@type': 'primary',                 # TODO: support other controller types
                    },
                    'openflow:group': groups,
                    'openflow:match': matches,
                }

                # XXX There is currently only 1 sliver per slice 
                rspec_dict['rspec']['openflow:sliver'] = sliver

        return rspec_dict


if __name__ == '__main__':
                
    PATH          = '../data'
    OF_ADVERT     = '%s/ofelia-bristol-of.rspec'  % (PATH,)
    OF_REQUEST    = '%s/of_request_uob_all.rspec' % (PATH,)
    OF_MANIFEST   = '%s/of_manifest.rspec'        % (PATH,)

    #TEST_DICT = {
    #    'controller': 'http://my.controller:1234',
    #    'flowspaces': [ 
    #        {
    #            'matches': [],
    #            'groups': []
    #        }, {
    #            'matches': [],
    #            'groups': []
    #        }
    #     ]
    #}

    TEST_DICT = {
        'controller' : 'http://my.controller.url',
        'groups'     : [{
                'name'  : 'mygroup1',
                'ports' : [{'datapath': 'XXX', 'port': 'XXX'}]
            }],
        'matches'    : [{
                'groups': ['mygroup1'],
                'packet': {'dl_type': 'XXX', 'nw_dst': 'XXX'}
            }]
    }

    def test_parse_all():
        for rspec_file in [OF_ADVERT, OF_REQUEST, OF_MANIFEST]:
            print "PARSING: %s" % (rspec_file,)
            try:
                ret = test_parse(rspec_file)
                for resource in ret['resource']:
                    print " - ", resource
                print "===="
            except Exception, e:
                import traceback
                traceback.print_exc()
                print "E: %s" % e

    def test_parse(rspec_file):
        parser = OfeliaOcfParser()
        return parser.parse(rspec_file)

    def test_build_request():
        parser = OfeliaOcfParser()
        print parser.build_rspec('onelab.upmc.test_fibre', TEST_DICT, list())

    test_parse_all()
    test_build_request()
