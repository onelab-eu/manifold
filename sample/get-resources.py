#!/usr/bin/env python
# -*- coding: utf-8 -*-

MYSLICE_API='http://localhost:7080'

# Connection to XMLRPC server
import xmlrpclib
srv = xmlrpclib.ServerProxy(MYSLICE_API, allow_none=True)

# Authentication token
auth = {"AuthMethod": "password", "Username": "jordan.auge@lip6.fr", "AuthString": "69803641"}

def print_slice(result):
    print "SLICE: %s" % result['slice_hrn']
    cpt = 0
    for i in result['resource']:
        if not 'sliver' in i:
            continue
        if cpt == 5:
            break
        asn = i['asn'] if 'asn' in i else 'None'
        country = i['country'] if 'country' in i else 'None'
        print "  - %s %s %s" % (i['hrn'], country, asn)
        cpt += 1
    print "    (only 5 first displayed)"

def print_result(result):
    cpt = 0
    for i in result:
        if cpt == 5:
            break
        print_slice(i)
        cpt += 1
    print "(only 5  displayed)"
    print "============================="


res =  srv.Get(auth, "resource", [["hostname", "=", "ple6.ipv6.lip6.fr"]], {}, ["hostname", "city"])

print_result(res)
