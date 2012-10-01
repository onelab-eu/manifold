#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

query = ('get', 'slice', [], {}, ['slice_hrn', 'resource.hostname'])

def print_result(result):
    cpt = 0
    for i in result:
        if cpt == 5:
            break
        print "[%d] " % cpt, i
        cpt += 1
    print "... (only 5 first displayed)"
    print "============================="


from tophat.core.router import THLocalRouter
from tophat.core.router import THQuery

# Instantiate a TopHat router
with THLocalRouter() as router:
    result = router.forward(THQuery(*query))
    print_result(result)
