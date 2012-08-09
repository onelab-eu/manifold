#!/usr/bin/env python
# -*- coding:utf-8 */

from tophat.core.router import THQuery, THLocalRouter as Router

query1 = THQuery(action='get', fact_table='tophat:platform', filters=[], params=None, fields=['platform', 'platform_longname'])
query2 = THQuery(action='get', fact_table='tophat:user')
query3 = THQuery(action='get', fact_table='tophat:account')

update = THQuery(action='update', fact_table='tophat:platform', params={'platform_longname': 'PlanetLab Europe testbed'}, filters=[['platform', '=', '''ple''']], fields=['platform', 'platform_longname'])

# Instantiate a TopHat router
with Router() as router:
    try:
        for q in [query1, query2, query3, update, query1]:
            print "=== Query: %s ===" % q
            result = router.forward(q)
            print result
    except Exception, e:
        print "Exception", e
