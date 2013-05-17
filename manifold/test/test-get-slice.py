#!/usr/bin/env python
#! -*- coding: utf-8 -*-

from manifold.auth        import *
from manifold.core.router import Router
from manifold.core.query  import Query
from config               import auth
import sys, pprint

DEFAULT_SLICE = 'ple.upmc.myslicedemo'

def print_err(err):
    print '-'*80
    print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
    for line in err['traceback'].split("\n"):
        print "\t", line
    print ''

slicename = sys.argv[1] if len(sys.argv) > 2 else DEFAULT_SLICE
query = Query.get('slice').filter_by('slice_hrn', '=', slicename).select([
    'slice_hrn',
    'resource.hrn', 'resource.hostname', 'resource.type', 'resource.authority',
    'user.user_hrn',
#    'application.measurement_point.counter'
])

with Router() as router:
    ret = router.forward(query, user=Auth(auth).check())

if ret['code'] != 0:
    if isinstance(ret['description'], list):
        # We have a list of errors
        for err in ret['description']:
            print_err(err)

ret = ret['value']

print "===== RESULTS ====="
for r in ret:
    r['resource'] = r['resource'][0:min(2, len(r['resource']))]
    if 'lease' in r:
        r['lease'] = r['lease'][0:min(2, len(r['lease']))]
    print "I: Limiting subquery results to 2 max..."
    pprint.pprint(r)
