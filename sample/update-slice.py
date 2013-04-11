#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

query = ('update', 'slice', [['slice_hrn', '=', 'ple.upmc.myslicedemo']], {'resource': ["ple.upmc.ple5\\.ipv6\\.lip6\\.fr"], 'lease': []}, ['slice_hrn', 'resource.hostname'])

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

from manifold.core.router import THRouter
from manifold.core.router import Query

# Instantiate a TopHat router
with THRouter() as router:
    user = router.authenticate({'AuthMethod': 'password', 'Username': 'jordan.auge@lip6.fr', 'password': 'demo'})
    result = router.forward(Query(*query), execute=True, user=user)
    print_result(result)
