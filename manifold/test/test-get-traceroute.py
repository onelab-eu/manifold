#!/usr/bin/env python
#! -*- coding: utf-8 -*-

#from manifold.auth        import *
from manifold.core.router import Router
from manifold.core.query  import Query
from config               import auth
import pprint

auth = {
    "AuthMethod" : "password",
    "Username"   : "guest@top-hat.info",
    "AuthString" : "guest"
}

def print_err(err):
    print "-" * 80
    print "Exception %(code)s raised by %(origin)s: %(description)s" % err
    for line in err["traceback"].split("\n"):
        print "\t", line
    print ""

query = Query.get("traceroute").select([
select = [
    "src_ip",
    "src_hostname",
    "dst_ip",
    "dst_hostname"#,
#    "hops.ip",
#    "hops.ttl",
#    "hops.hostname"
]).filter_by([
    ("src_ip", "=", 141.22.213.34),
    ("dst_ip", "=", "139.91.90.239") # or 195.116.60.211
]).at("2012-09-09 14:30:09")

print query

ret = Router().forward(query, user = Auth(auth).check())

if ret["code"] != 0:
    if isinstance(ret["description"], list):
        # We have a list of errors
        for err in ret["description"]:
            print_err(err)

ret = ret["value"]

print "===== RESULTS ====="
for r in ret:
    pprint.pprint(r)
