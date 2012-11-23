#!/usr/bin/env python
# -*- coding:utf-8 */

import sys, xmlrpclib, os, re
from config                 import auth
from tophat.models.platform import Platform
from tophat.core.router     import THLocalRouter
from tophat.core.router     import Query
from tophat.models          import db

#======================================================================

import xml.etree.cElementTree as ElementTree
from tophat.util.xmldict import XmlDictConfig
from tophat.core.table import Table

#query = Query(
#    # action
#    'get',
#    # from (= query.fact_table)
#    'slice',
#    # where (= query.filters)
#    [['slice_hrn', '=', 'ple.upmc.myslicedemo']],
#    # query.params
#    {},
#    # select (= query.fields)
#    ['slice_hrn', 'resource.hrn', 'resource.country', 'resource.asn', 'lease.duration']
#)

query = Query(
    # action
    'get',
    # from (= query.fact_table)
    'traceroute',
    # where (= query.filters)
    [
        ["src_ip", "=", "141.22.213.34"],
        ["dst_ip", "=", "139.91.90.239"]
    ],
    # query.params
    {},
    # select (= query.fields)
    ["src_ip", "dst_ip", "hops.ttl", "hops.ip", "hops.hostname"] ,
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
    result = router.forward(query, execute = True, user = user)
    print result
