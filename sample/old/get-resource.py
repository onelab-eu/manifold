#!/usr/bin/env python
# -*- coding: utf-8 -*-

MYSLICE_API='http://demo.myslice.info:7080'

# Connection to XMLRPC server
import xmlrpclib
srv = xmlrpclib.ServerProxy(MYSLICE_API, allow_none=True)

# Authentication token
auth = {"AuthMethod": "password", "Username": "jordan.auge@lip6.fr", "AuthString": "69803641"}


def print_result(result):
    cpt = 0
    for i in result:
        if cpt == 5:
            break
        print i
        cpt += 1
    print "(only 5  displayed)"
    print "============================="


res =  srv.Get(auth, "resource", [["hostname", "=", "planet3.cs.huji.ac.il"]], {}, ["hostname", "city"])

print_result(res)
