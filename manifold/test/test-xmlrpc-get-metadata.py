#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import xmlrpclib, pprint
from config import auth

srv = xmlrpclib.Server("https://localhost:7080/", allow_none = True)

def print_err(err):
    print '-'*80
    print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
    for line in err['traceback'].split("\n"):
        print "\t", line
    print ''

q = {
    'object': 'local:object',
    #'filters': [['table', '==', 'asn']]
}

ret = srv.forward(q, auth)

if ret['code'] != 0:
    if isinstance(ret['description'], list):
        # We have a list of errors
        pprint.pprint(ret)

ret = ret['value']

print "===== RESULTS ====="
pprint.pprint(ret['value'])

