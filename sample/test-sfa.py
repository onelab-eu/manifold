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

q = {'action': 'get', 'fields': ['network_hrn'], 'object': 'network', 'params': [], 'filters': []}

q = {
    'object':   'network',
    'fields':       ['network_hrn']
}

q_platform = {
    'object':   'local:platform', 
    'fields':       ['platform', 'platform_description']
}

#q_update = {
#    'action': 'update',
#    'object': 'local:platform',
#    'filters': [['platform','=','ple']],
#    'params': {'platform_description':'test'}
#}

#ret = srv.forward(auth, q_platform)
#print "====> PLATFORM"
#for r in ret:
#    print r
#print ret

ret = srv.forward(auth, q)
print "====> NETWORKS"
#print ret
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
#for r in ret:
    #print r
    #q_update = {
    #    'action': 'update',
    #    'object': 'local:platform',
    #    'filters': [['platform','=',r['hrn']]],
    #    'params': {'platform_description':r['hostname']}
    #}
    # update platforms set platform_description=r['hostname'] where platform=r['hrn']
    #ret_update = srv.forward(auth, q_update)
    #print "====> UPDATE = ", q_update
    #for r in ret:
    #    print r
    #print ret_update
