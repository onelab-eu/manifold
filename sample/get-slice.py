#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

from config import auth

query = ('get', 'slice', [['slice_hrn', '=', 'ple.upmc.myslicedemo']], {}, ['slice_hrn', 'resource.hrn', 'resource.country', 'resource.asn', 'lease.duration'])

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

def print_result(result):
    cpt = 0
    for i in result:
        if cpt == 5:
            break
        print_slice(i)
        cpt += 1
    print "(only 5  displayed)"
    print "============================="


from tophat.core.router import THLocalRouter
from tophat.core.router import Query

# Instantiate a TopHat router
with THLocalRouter() as router:
    user = router.authenticate(auth)
    result = router.forward(Query(*query), execute=True, user=user)
    print_result(result)
