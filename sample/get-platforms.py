#!/usr/bin/env python
# -*- coding:utf-8 */

from tophat.core.router import THQuery, THLocalRouter as Router

query1 = THQuery('get', 'tophat:user', [], [])
query2 = THQuery('get', 'tophat:platform', [], ['platform', 'platform_longname'])
query3 = THQuery('get', 'tophat:account', [], [])

# Instantiate a TopHat router
with Router() as router:
    for q in [query1, query2, query3]:
        result = router.forward(q)
        print result
