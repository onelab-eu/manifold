#!/usr/bin/env python
# -*- coding:utf-8 */

from config                 import auth
from tophat.core.router     import THLocalRouter
from tophat.util.predicate  import Predicate 
from manifold.core.query    import Query

query = Query(
    "get",
    "x",
    [Predicate("x", "=", "1")],
    {},
    ["x", "t"],
    None
)

## Require type inference
#query = Query(
#    # action
#    "get",
#    # from
#    "traceroute",
#    # where
#    [
#        ["source.ip",      "=", "141.22.213.34"],
#        ["destination.ip", "=", "139.91.90.239"]
#    ],
#    # query.params
#    {},
#    # select (= query.fields)
#    #["source.ip", "destination.ip", "hops.ttl", "hops.ip", "hops.hostname"] ,
#    ["source.ip", "destination.ip"],
#    # timestamp
#    "2012-09-09 14:30:09"
#)
#
#query = Query(
#    # action
#    "get",
#    # from
#    "hop",
#    # where
#    [],
#    # query.params
#    {},
#    # select (= query.fields)
#    ["ip", "ttl", "hostname"] ,
#    #["source.ip", "destination.ip"],
#    # timestamp
#    "2012-09-09 14:30:09"
#)
#query = Query(
#    # action
#    "get",
#    # from
#    "x",
#    # where
#    [],
#    # query.params
#    {},
#    # select
#    ["x", "y", "z", "t"] ,
#    #["source.ip", "destination.ip"],
#    # timestamp
#    "2012-09-09 14:30:09"
#)


print "=" * 150
print "%r" % query
print "> action = %r" % query.get_action()
print "> select = %r" % query.get_select()
print "> from   = %r" % query.get_from()
print "> where  = %r" % query.get_where()
print "> params = %r" % query.get_params()
print "=" * 150

# Instantiate a TopHat router
with THLocalRouter() as router:
    user = router.authenticate(auth)
    #directory = router.conf.STATIC_ROUTES_FILE
    #print router.get_static_routes(directory)
    result = router.forward(query, execute = False, user = user)
    if result:
        print "result =", result
