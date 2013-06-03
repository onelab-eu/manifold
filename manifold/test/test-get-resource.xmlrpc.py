#!/usr/bin/env python
import xmlrpclib
srv = xmlrpclib.ServerProxy("http://dev.myslice.info:7080/", allow_none=True)
#auth = {"AuthMethod": "password", "Username": "thierry", "AuthString": "thierry"}
from manifold.test.config import auth 
slicename= "ple.inria.heartbeat"

q = {
    'action' : 'get',
    'object' : 'resource',
#    'filters': [["slice_hrn", "==",slicename]],
    'fields' : ["network", "type", "resource_hrn", "hostname"]
}
rs=srv.forward(auth, q)

print rs

print 'received',len(rs),'resources attached to',slicename

