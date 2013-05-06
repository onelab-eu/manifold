#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib
from config                   import auth
from manifold.core.router     import Router
from manifold.util.predicate  import Predicate
from manifold.core.query      import Query
query1 = ('get', 'network', [], {}, ['network_hrn'],'now')
#query2 = ('get', 'node', [], {}, ['hostname', 'asn', 'city'])


def print_result(result):
    cpt = 0
    for i in result:
        if cpt == 5:
            break
        print "[%d] " % cpt, i
        cpt += 1
    print "... (only 5 first displayed)"
    print "============================="

#from manifold.core.router import THRouter
#from manifold.core.router import THQuery

# Instantiate a TopHat router
with Router() as router:
    # TODO How to make it work without __enter__ __exit__ ??
    #router = Router()

    for query in [query1]: #query1, query2]:
        result = router.forward(Query(*query))
        print(result)
