#!/usr/bin/env python
# -*- coding: utf-8 -*-

import xmlrpclib, pprint

# This is to avoid displaying our authentication tokens
from manifold.test.config import auth 

srv = xmlrpclib.ServerProxy("https://dev.myslice.info:7080/", allow_none=True)

# We formulate a query
query = {
    'action' : 'get',
    'object' : 'resource',
    'fields' : ["network", "hrn"]
}
rs = srv.forward(query, {'authentication': auth})

print rs
