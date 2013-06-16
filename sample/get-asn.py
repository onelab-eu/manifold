#!/usr/bin/env python
#! -*- coding: utf-8 -*-
import sys, xmlrpclib, pprint

auth = {'AuthMethod': 'password', 'Username': 'demo', 'AuthString': 'demo'}

err_str = {
    0: "OK",
    1: "WARNING",
    2: "ERROR"
}

srv = xmlrpclib.Server("http://dev.myslice.info:7080/", allow_none = True)

def print_err(err):
    print '-'*80
    print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
    for line in err['traceback'].split("\n"):
        print "\t", line
    print ''


asn_list = open(sys.argv[1]).readlines()
asn_list = map(lambda x:int(x.rstrip()), asn_list)

q = {
    'object':   'asn',
    'filters':  [['asn', '{', asn_list]],
    'fields':   ['asn', 'asn_class', 'asn_description']
}

ret = srv.forward(auth, q)

if 'description' in ret:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)

if 'value' in ret:
    for r in ret['value']:
        print r['asn'], r['asn_description'], r['asn_class']
