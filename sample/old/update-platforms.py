#!/usr/bin/env python
# -*- coding:utf-8 */

from manifold.core.router import Query, Router

q = Query(action='get', object='local:platform', filters=[], params=None, fields=['platform', 'platform_longname'])

# Update(object, filters, params, fields, callback)
query = Query(action='update', object='local:platform', params={['platform_description', '=', 'test']}, filters=[['platform','=','ple']])

# Instantiate a TopHat router
with Router() as router:
    try:
        print "=== Query: %s ===" % query
        result = router.forward(query)
        print result

        print "=== Query: %s ===" % q
        result = router.forward(q)
        print result

        #for r in result:
            
    except Exception, e:
        print "Exception", e
