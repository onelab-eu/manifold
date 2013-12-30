#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import xmlrpclib
srv = xmlrpclib.Server("http://localhost:58000/RPC/", allow_none = True)
print srv.system.listMethods()
print srv.forward({'object': 'traceroute'}, {})
