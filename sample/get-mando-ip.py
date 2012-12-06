#!/usr/bin/env python
# -*- coding:utf-8 */

from config                 import auth
from tophat.core.router     import THLocalRouter
from tophat.core.router     import Query

query = Query(
    # action
    'get',
    # from (= query.fact_table)
    'ip',
    # where (= query.filters)
    [
        ["ip", "=", "141.22.213.34"]
    ],
    # query.params
    {},
    # select (= query.fields)
    ["ip", "delta"] ,
    # timestamp
    "2012-09-09 14:30:09"
)


print "=" * 150
print query
print "> action     = %r" % query.action
print "> fact_table = %r" % query.fact_table
print "> filters    = %r" % query.filters
print "> params     = %r" % query.params
print "> fields     = %r" % query.fields
print "=" * 150

# Instantiate a TopHat router
with THLocalRouter() as router:
    user = router.authenticate(auth)
    directory = router.conf.STATIC_ROUTES_FILE
    print router.get_static_routes(directory)
    result = router.forward(query, execute = False, user = user)
    print result
