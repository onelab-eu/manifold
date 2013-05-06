#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

from config import auth

query = ('get', 'slice', [['slice_hrn', '=', 'ple.upmc.myslicedemo']], {'debug': True}, ['slice_hrn', 'resource.hrn', 'resource.country', 'resource.asn', 'lease.duration'], 'now')

def print_slice(result):
    print "SLICE: %s" % result['slice_hrn']
    cpt = 0
    for i in result['resource']:
        if not 'sliver' in i:
            continue
        if cpt == 5:
            break
        asn = i['asn'] if 'asn' in i else 'None'
        country = i['country'] if 'country' in i else 'None'
        print "  - %s %s %s" % (i['hrn'], country, asn)
        cpt += 1
    print "    (only 5 first displayed)"
    print "--- <DEBUG> ---"
    print "    RSpec:"
    print "    ", result['debug']['rspec'][0:50], '...'
    print "--- </DEBUG> ---"

def print_result(result):
    cpt = 0
    for i in result:
        if cpt == 5:
            break
        print_slice(i)
        cpt += 1
    print "(only 5  displayed)"
    print "============================="


from manifold.core.router import THRouter
from manifold.core.router import Query

# Instantiate a TopHat router
with THRouter() as router:
    user = router.authenticate(auth)
    result = router.forward(Query(*query), execute=True, user=user)
    #print_result(result)
