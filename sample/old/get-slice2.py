#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

query = ('get', 'slice', [], {}, ['slice_hrn', 'resource.hostname', 'resource.city'])


def print_slice(result):
    print "SLICE: %s" % result['slice_hrn']
    cpt = 0
    for i in result['resource']:
        if cpt == 5:
            break
        print "  - %s - %s " % (i['hostname'], i['city'])
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
    result = router.forward(Query(*query)) #, execute=False)
    if result:
        print ""
        print "=== RESULT ==="
        print_result(result)
        print "--------------"
        print ""
