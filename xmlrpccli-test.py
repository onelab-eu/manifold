#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import xmlrpclib
srv = xmlrpclib.Server("https://localhost:7080/", allow_none = True)
print srv.forward({'object': 'traceroute'}, {})
