#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

query = ('get', 'metadata:table', [], {}, [], 'latest')

from tophat.core.router import THLocalRouter
from tophat.core.router import Query

# Instantiate a TopHat router
with THLocalRouter() as router:
    result = router.forward(Query(*query), execute=True)
    for x in result:
        print x['table']
        for c in x['column'][0:5]:
            print "  - %s" % c['column']
