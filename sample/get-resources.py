#!/usr/bin/env python
# -*- coding:utf-8 */

import sys
import xmlrpclib

MYSLICE_API = "http://demo.myslice.info:7080/API/"

query1 = ('get', 'nodes', [['country', '=', 'France']], ['hostname', 'arch', 'country'])
query2 = ('get,' 'resources', [], ['hostname', 'asn', 'city'])


def print_result(result):
    cpt = 0
    for i in result:
        if cpt == 5:
            break
        print "[%d] " % cpt, i
        cpt += 1
    print "... (only 5 first displayed)"
    print "============================="

#auth = {'AuthMethod': 'password', 'Username': 'demo', 'AuthString': 'demo'}

MySlice = xmlrpclib.Server(MYSLICE_API, allow_none = True)

for query in [query1, query2]:
    result = MySlice.Get(*query)
    print result

#from tophat.core.router import THLocalRouter as Router
#from tophat.core.router import THQuery as Query
#
## Instantiate a TopHat router
#with Router() as router:
#    # TODO How to make it work without __enter__ __exit__ ??
#    #router = Router()
#
#    for query in [query1, query2]:
#        print "I: Query %s" % query
#        try:
#            result = router.forward(Query(query))
#        except Exception, e:
#            result = []
#            print 'Exception', e
#
#        print_result(result)
