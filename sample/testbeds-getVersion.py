#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import xmlrpclib
from config import auth

srv = xmlrpclib.Server("http://localhost:7080/", allow_none = True)
#srv = xmlrpclib.Server("http://dev.myslice.info:7080/", allow_none = True)

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
q = {
    'fact_table':   'network',
    'fields':       ['network_hrn']
}
ret = srv.forward(auth, q)
print "====> NETWORK"
#print ret

if 'code' in ret.keys() and ret['code']!= 0:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)
else:
    ret = ret['result']
    print "===== RESULTS ====="
    #for r in ret:
    print ret
#    q_update = {
#        'action': 'update',
#        'fact_table': 'local:platform',
#        'filters': [['platform','=',r['hrn']]],
#        'params': {'platform_description':r['hostname']}
#    }
    # update platforms set platform_description=r['hostname'] where platform=r['hrn']
    #ret_update = srv.forward(auth, q_update)
    #print "====> UPDATE = ", q_update
    #print ret_update
