#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

query = ('get', 'slice', [['slice_hrn', '=', 'ple.upmc.agent']], {}, ['slice_hrn', 'resource.hostname'])


def print_slice(result):
    print "SLICE: %s" % result['slice_hrn']
    cpt = 0
    for i in result['resource']:
        if cpt == 5:
            break
        print "  - %s" % i['hostname']
        cpt += 1
    print "    (only 5 first displayed)"

def print_result(result):
    cpt = 0
    for i in result:
        if cpt == 5:
            break
        print_slice(i)
        cpt += 1
    print "(only 5 first displayed)"
    print "============================="


from tophat.core.router import THLocalRouter
from tophat.core.router import Query

# Instantiate a TopHat router
with THLocalRouter() as router:
    user = router.authenticate({'AuthMethod': 'password', 'Username': 'demo', 'password': 'demo'})
    result = router.forward(Query(*query), execute=True, user=user)
    print "=== RESULT ==="
    print_result(result)
    print "--------------"
    print ""
