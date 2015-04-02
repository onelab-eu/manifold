#!/usr/bin/env python
import xmlrpclib
srv = xmlrpclib.ServerProxy("https://cetus.ipv6.lip6.fr:7080/", allow_none=True)
auth = {"AuthMethod": "password", "Username": "demo", "AuthString": "demo"}

q = {
    'action' : 'get',
    'object' : 'availability',
    'filters': [['last_check', '>', '2015-03-09 19:09:01 +0300']],
    'fields' : ["*"]
}
rs=srv.forward(q,{'authentication':auth})

print rs

print 'received',len(rs)

