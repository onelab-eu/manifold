#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

query = ('get', 'slice', [['slice_hrn', '=', 'ple.upmc.myslicedemo']], {}, ['slice_hrn', 'resource.hrn', 'resource.country', 'lease.duration'])

def print_slice(result):
    print "SLICE: %s" % result['slice_hrn']
    cpt = 0
    for i in result['resource']:
        if cpt == 5:
            break
        print "  - %s %s" % (i['hrn'], i['country'])
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
    user = router.authenticate({'AuthMethod': 'password', 'Username': 'jordan.auge@lip6.fr', 'password': 'demo'})
    result = router.forward(Query(*query), execute=True, user=user)
    print "=== RESULT ==="
    print_result(result)
    print "--------------"
    print ""
