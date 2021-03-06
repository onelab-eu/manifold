#!/usr/bin/env python
#! -*- coding: utf-8 -*-

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
    'object':   'slice',
    'filters':      [['slice_hrn', '=', 'ple.upmc.myslicedemo']],
    'fields':       ['slice_hrn', 'resource.hrn', 'resource.hostname']
}

ret = srv.forward(auth, q)
if ret['code'] != 0:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)

ret = ret['value']

print "===== RESULTS ====="
for r in ret:
    print r['slice_hrn']
    for rr in r['resource']:
        if not 'hostname' in rr:
            print rr
        else:
            print "    >", rr['hostname']
    print 
