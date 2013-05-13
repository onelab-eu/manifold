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

q_account = {
    'action': 'get', 
    'object': 'local:account', 
    'filters': [['platform', '=', 'ple'], ['email', '=', 'loic.baron@lip6.fr']]
}
test = {'user_hrn':'loic','cred':'test'}
q_update = {
    'action': 'update',
    'object': 'local:account', 
    'filters': [['platform', '=', 'ple'], ['user', '=', 'loic.baron@lip6.fr']],
    'params': {'config':test}
}

ret = srv.forward(auth, q_account)
print "====> ACCOUNT"
for r in ret:
    print r
print ret

if 'code' in ret.keys() and ret['code'] != 0:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)

ret = ret['result']
