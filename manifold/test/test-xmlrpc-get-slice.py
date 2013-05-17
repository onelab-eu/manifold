#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys, xmlrpclib
from config import auth

srv = xmlrpclib.Server("http://localhost:7080/", allow_none = True)

def print_err(err):
    print '-'*80
    print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
    for line in err['traceback'].split("\n"):
        print "\t", line
    print ''

DEFAULT_SLICE = 'ple.upmc.myslicedemo'
slicename = sys.argv[1] if len(sys.argv) > 2 else DEFAULT_SLICE
query = {
    'object'    : 'slice',
    'filters'   : [['slice_hrn', '=', slicename]],
    'fields'    : ['slice_hrn', 
                   'resource.hrn', 'resource.hostname', 'resource.type', 'resource.authority',
                   'user.user_hrn']
}

ret = srv.forward(auth, query)
if ret['code'] != 0:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)

ret = ret['value']

print "===== RESULTS ====="
for r in ret:
    print r

