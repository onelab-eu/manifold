#!/usr/bin/env python
# -*- coding:utf-8 */

#MYSLICE_API = "https://demo.myslice.info/API/"
#auth = {'AuthMethod': 'password', 'Username': 'demo', 'AuthString': 'demo'}
#
#import sys
#import xmlrpclib
#
#MySlice = xmlrpclib.Server(MYSLICE_API, allow_none = True)
#result = MySlice.Get(auth, 'resources', 'now', { 'country': 'Netherlands'}, ['hostname', 'arch', 'country'])

from tophat.core.router import THLocalRouter as Router
from tophat.core.router import THQuery as Query

# Instantiate a TopHat router
with Router() as router:
    # TODO How to make it work without __enter__ __exit__ ??
    #router = Router()

    query1 = Query('nodes', [['country', '=', 'France']], ['hostname', 'arch', 'country'])
    query2 = Query('resources', [], ['hostname', 'asn', 'city'])

    for query in [query1, query2]:
        print "I: Query %s" % query
        try:
            result = router.forward(query)
        except Exception, e:
            result = []
            print 'Exception', e

        # ... and returs a table
        cpt = 0
        for i in result:
            if cpt == 5:
                break
            print "[%d] " % cpt, i
            cpt += 1
        print "... (only 5 first displayed)"
        print "============================="
