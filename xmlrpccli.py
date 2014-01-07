#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import xmlrpclib
auth = {'AuthMethod': 'anonymous'}
srv = xmlrpclib.Server("https://dryad.ipv6.lip6.fr:7080/", allow_none = True)
print srv.AuthCheck({'authentication': auth})
