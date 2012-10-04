#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

#query1 = ('get', 'nodes', [['country', '=', 'France']], {}, ['hostname', 'arch', 'country'])
query2 = ('get', 'resources', [], {}, ['hostname', 'asn', 'city'])


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
from tophat.core.query import Query

# Instantiate a TopHat router
with THLocalRouter() as router:
    # TODO How to make it work without __enter__ __exit__ ??
    #router = Router()

    for query in [query2]: #query1, query2]:
        result = router.forward(Query(*query))
        print_result(result)
