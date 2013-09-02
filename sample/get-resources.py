#!/usr/bin/env python
#! -*- coding: utf-8 -*-
import pprint
import xmlrpclib
import sys

try:
    from config import auth
except ImportError:
    print """
    Please create manifold/sample/config.py and write in this file:

       auth = {'AuthMethod': 'password', 'Username': 'my_email_address', 'AuthString': 'my_password'}"

    """
    sys.exit(-1)

err_str = {
    0: "OK",
    1: "WARNING",
    2: "ERROR"
}

srv = xmlrpclib.Server("http://localhost:7080/", allow_none = True)

def print_err(err):
    print '-'*80
    print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
    for line in err['traceback'].split("\n"):
        print "\t", line
    print ''

q = {
    'object':   'resource',
    'fields':   ['resource_hrn']
    #'filter':   [[]]
}

ret = srv.forward(auth, q)
print "====> RESOURCES"
pprint.pprint(ret)

print "RETURN VALUE:", err_str[ret['code']]

if 'description' in ret:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)
    else:
        print ret['description']

if 'value' in ret:
    print "***** Result:"
    pprint.pprint(ret['value'])
