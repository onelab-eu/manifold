#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import xmlrpclib
srv = xmlrpclib.Server("http://ple2.ipv6.lip6.fr:58000/RPC/", allow_none = True)
print srv.forward({'object': 'traceroute', 'filters': [['destination', '==', '8.8.8.8']]})
