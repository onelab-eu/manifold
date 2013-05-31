#!/usr/bin/env python
#! -*- coding: utf-8 -*-

from manifold.auth        import *
from manifold.core.router import Router
from manifold.core.query  import Query
from config               import auth
import sys, pprint

def print_err(err):
    print '-'*80
    print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
    for line in err['traceback'].split("\n"):
        print "\t", line
    print ''

query = Query.get('local:user')

ret = Router().forward(query, user=Auth(auth).check())

if ret['code'] != 0:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)

ret = ret['value']

print "===== RESULTS ====="
for r in ret:
    pprint.pprint(r)
