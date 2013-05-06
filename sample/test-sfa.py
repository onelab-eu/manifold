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
    'fact_table':   'network',
    'fields':       ['network_hrn']
}

q_platform = {
    'fact_table':   'local:platform', 
    'fields':       ['platform', 'platform_description']
}

q_update = {
    'action': 'update',
    'fact_table': 'local:platform',
    'filters': [['platform','=','ple']],
    'params': {'platform_description':'test'}
}

ret = srv.forward(auth, q_platform)
print "====> PLATFORM"
#for r in ret:
#    print r
print ret

#ret = srv.forward(auth, q_update)
#print "====> UPDATE"
#for r in ret:
#    print r
#print ret

ret = srv.forward(auth, q)
if ret['code'] != 0:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)

ret = ret['result']

print "===== RESULTS ====="
for r in ret:
    print r
    # update platforms set platform_description=r['hostname'] where platform=r['hrn']
