#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import xmlrpclib
from config import auth

srv = xmlrpclib.Server("http://localhost:7080/", allow_none = True)

q = [auth, 'test', [['b', ']', 2]], {}, ['a', 'b']]

print srv.forward(*q)

