#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid
from collections                    import OrderedDict
from datetime                       import datetime
from types                          import StringTypes

from manifold.util.log              import Log
from manifold.gateways.sfa.rspecs   import RSpecParser, RENAME, HOOKS

class OfeliaVTAMParser(RSpecParser):

    __namespace_map__ = {}

    #---------------------------------------------------------------------------
    # RSpec parsing
    #---------------------------------------------------------------------------
    __actions__ = {
        'rspec/node': {
            RENAME  : {
                '@component_id'        : 'component_id',
                '@component_manager_id': 'component_manager_id',
                '@exclusive'           : 'exclusive',
            },
            HOOKS   : [RSpecParser.xrn_hook, RSpecParser.type_hook('node'), RSpecParser.testbed_hook, RSpecParser.hostname_hook]
            # REMOVE_@
            # REMOVE_#
        },
        'rspec/network/node': {
            RENAME  : {
                '@component_id'        : 'component_id',
                '@component_manager_id': 'component_manager_id',
                '@exclusive'           : 'exclusive',
            },
            HOOKS   : [RSpecParser.xrn_hook, RSpecParser.type_hook('node'), RSpecParser.testbed_hook, RSpecParser.hostname_hook]
            # REMOVE_@
            # REMOVE_#
        },

    }

    @classmethod
    def parse_impl(cls, rspec_dict):
        resources     = list()
        leases        = list()
        vms           = dict()
        slivers_names = None
        urn           = None

        rspec = rspec_dict.get('rspec')
        if not rspec:
            rspec = rspec_dict.get('RSpec')

        network = rspec.get('network')

        if not network:
            nodes = rspec.get('node', list())
        else:
            nodes = network.get('node', list())

        print "PARSING VT =================="
        print rspec.get('@type') 

        if rspec.get('@type') == "manifest":
            # Each vm is embeded in the sliver returned in the manifest RSpec
            # this corresponds to the request RSpec sent by the experimenter
            #for node in nodes:
            if isinstance(nodes, list):
                for node in nodes:
                    urn, slivers_names = cls.get_sliver_names(node)
                    node = urn
            else:
                urn, slivers_names = cls.get_sliver_names(nodes)
                nodes = [urn]

            # Resources are already in the Slice
            if urn is not None and slivers_names is not None:
                vms[urn] = slivers_names
            # Slice is empty
            else:
                nodes = []

        resources.extend(nodes)

        return resources, leases, [vms]

    @classmethod
    def get_sliver_names(cls,node):
        slivers_names = list()
        slivers = node.get('sliver', list())
        for sliver in slivers:
            slivers_names.append(dict((key,value) for key, value in sliver.iteritems() if key == 'name'))
        urn = node.get('@component_id')
        return urn, slivers_names


    #---------------------------------------------------------------------------
    # RSpec construction
    #---------------------------------------------------------------------------

    __rspec_request_base_dict__ = {
        u'rspec': {
            u'@type': u'SFA', 
        }
    }

    @classmethod
    def build_rspec_impl(cls, slice_hrn, resources, leases, flowspaces):
        """
        Returns a dict used to build the request RSpec with xmltodict.

        NOTE: since ad/request/manifest are all different, it is difficult to
        automate translation between Manifold and RSpecs.
        """
        rspec_dict = cls.__rspec_request_base_dict__.copy()
        nodes = list()

        for node_urn in flowspaces:
            t_node_urn = node_urn.split('+')
            component_manager_id = t_node_urn[0] + '+' + t_node_urn[1] + '+authority+cm'
            node_dict = {
               u'@component_manager_id': component_manager_id,
               u'@component_id': node_urn,
            }

            slivers = list()
            for vm in flowspaces[node_urn]:
                sliver_dict = {
                    u'@name' : vm['name'],
                    u'@uuid' : 'myuuid',
                    u'@project-id' : 'myproject',
                    u'@slice-id' : slice_hrn,
                    u'@slice-name' : slice_hrn,
                    u'@operating-system-type' : 'GNU/Linux',
                    u'@operating-system-version' : '5.0',
                    u'@operating-system-distribution' : 'Debian',
                    u'@server-id' : 'serverid',
                    u'@hd-setup-type' : 'file-image',
                    u'@hd-origin-path' : 'default/test/lenny',
                    u'@virtualization-setup-type' : 'paravirtualization',
                    u'@memory-mb' : '128',
                }
                slivers.append(sliver_dict)

            node_dict['sliver'] = slivers

            nodes.append(node_dict)
        
        rspec_dict['rspec'] = {'node':nodes}
        return rspec_dict


if __name__ == '__main__':
                
    PATH          = '../data'
    OF_ADVERT     = '%s/VTAM_AD_RSPEC.xml'  % (PATH,)
    OF_REQUEST    = '%s/VTAM_REQ_RSPEC.xml' % (PATH,)
    #OF_MANIFEST   = '%s/of_manifest.rspec'  % (PATH,)

    TEST_DICT = {
                'urn:publicid:IDN+virtualization:i2cat:vtam+node+Martorell' : [{'name':'myVM_1'},{'name':'myVM_2'}],
                'urn:publicid:IDN+virtualization:i2cat:vtam+node+Serafi' : [{'name':'myVM_3'}],
                }

    def test_parse_all():
        for rspec_file in [OF_ADVERT, OF_REQUEST]:
            print "PARSING: %s" % (rspec_file,)
            try:
                rspec_file = open(rspec_file).read()
                ret = test_parse(rspec_file)
                print ret
                #for resource in ret['resource']:
                #    print " - ", resource
                print "===="
            except Exception, e:
                import traceback
                traceback.print_exc()
                print "E: %s" % e

    def test_parse(rspec_file):
        parser = OfeliaVTAMParser()
        return parser.parse(rspec_file)

    def test_build_request():
        parser = OfeliaVTAMParser()
        print parser.build_rspec('onelab.upmc.test_fibre', resources = list(), leases = list(), flowspace = TEST_DICT)

    test_parse_all()
    test_build_request()
