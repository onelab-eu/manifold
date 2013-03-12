#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import xmlrpclib
auth = {'AuthMethod': 'guest'}
srv = xmlrpclib.Server("http://localhost:7080/", allow_none = True)
print srv.AuthCheck()
