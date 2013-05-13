#!/usr/bin/env python
# -*- coding:utf-8 */

from manifold.core.router import Query, Router

query1 = Query(action='get', object='local:platform', filters=[], params=None, fields=['platform', 'platform_longname'])
query2 = Query(action='get', object='local:user')
query3 = Query(action='get', object='local:account')

update = Query(action='update', object='local:platform', params={'platform_longname': 'PlanetLab Europe testbed'}, filters=[['platform', '=', '''ple''']], fields=['platform', 'platform_longname'])

# Instantiate a TopHat router
with Router() as router:
    try:
        for q in [query1, query2, query3, update, query1]:
            print "=== Query: %s ===" % q
            result = router.forward(q)
            print result
    except Exception, e:
        print "Exception", e
