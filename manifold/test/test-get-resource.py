#!/usr/bin/env python
#! -*- coding: utf-8 -*-

from manifold.auth              import *
from manifold.bin.shell         import Shell
from manifold.core.router       import Router
from manifold.core.query        import Query
from manifold.core.result_value import ResultValue
from manifold.util.options      import Options
from config                     import auth
import sys, pprint

def assert_rv_success(result_value):
    print type(result_value)
    if isinstance(result_value, dict):
        return result_value['value']
    assert isinstance(result_value, ResultValue)
    assert result_value.is_success()
    records = result_value.get_value()
    #assert isinstance(records, Records) # ONLY IN ROUTERV2
    return records

#Options().log_level = 'DEBUG'
Options().username = auth['Username']
Options().password = auth['AuthString']
Options().xmlrpc_url = "https://portal.onelab.eu:7080"

shell = Shell(interactive=False)
shell.select_auth_method('password')

command = 'SELECT hrn, hostname FROM resource'
result_value = shell.evaluate(command)
try:
    records = assert_rv_success(result_value)
    print "===== RESULTS ====="
    print records

except Exception, e:
    print "===== ERROR ====="
    import traceback
    traceback.print_exc()
    print e

shell.terminate()
#def print_err(err):
#    print '-'*80
#    print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
#    for line in err['traceback'].split("\n"):
#        print "\t", line
#    print ''
#
# query = Query.get('resource').select(['resource_hrn', 'hostname'])
#
# # manage twisted reactor (thread) necessary for asynchronous processing
# with Router() as router:
#     ret = Router().forward(query, user=Auth(auth).check())
#
#if ret['code'] != 0:
#    if isinstance(ret['description'], list):
#        # We have a list of errors
#        for err in ret['description']:
#            print_err(err)
#    else:
#        print ret
#
#ret = ret['value']
#
#print "===== RESULTS ====="
#for r in ret:
#    pprint.pprint(r)
