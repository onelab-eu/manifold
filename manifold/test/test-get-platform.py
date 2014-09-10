#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import xmlrpclib
from config import auth

#srv = xmlrpclib.Server("http://dev.myslice.info:7080/", allow_none = True)
srv = xmlrpclib.Server("https://localhost:7080/", allow_none = True)

def print_err(err):
    print '-'*80
    print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
    for line in err['traceback'].split("\n"):
        print "\t", line
    print ''

q = {
    'object':   'local:platform',
    'fields':   ['platform', 'platform_description']
}
ret = srv.forward(q, {'authentication': auth})
# DEPRECATED | ret = srv.forward(auth, q)
if ret['code'] != 0:
    if isinstance(ret['description'], list):
        print "there is an error"
        # We have a list of errors
        for err in ret['description']:
            print_err(err)
    else:
        print ret

ret = ret['value']

print "===== RESULTS ====="
for r in ret:
    print r
