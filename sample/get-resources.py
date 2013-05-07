#!/usr/bin/env python
#! -*- coding: utf-8 -*-
import pprint
import xmlrpclib
from config import auth

srv = xmlrpclib.Server("http://localhost:7080/", allow_none = True)

def print_err(err):
    print '-'*80
    print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
    for line in err['traceback'].split("\n"):
        print "\t", line
    print ''

q = {
    'fact_table':   'resource',
    'fields':       ['hrn']
}

ret = srv.forward(auth, q)
print "====> RESOURCES"
#pprint.pprint(ret)

if 'code' in ret.keys() and ret['code'] != 0:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)
    print "===== RETURN WITH ERRORS ====="
    pprint.pprint(ret)
else:
    print "===== RESULTS ====="
    pprint.pprint(ret)
    #ret = ret['result']
