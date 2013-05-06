#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

query = ('get', 'metadata:table', [], {}, [], 'latest')

from manifold.core.router import Router
from manifold.core.router import Query

# Instantiate a TopHat router
with Router() as router:
    result = router.forward(Query(*query), execute=True)
    #print result
    if result['code']==0:
        for x in result['result']:
            #print x
            print x['table']
            for c in x['column'][0:5]:
                print "  - %s" % c['column']
