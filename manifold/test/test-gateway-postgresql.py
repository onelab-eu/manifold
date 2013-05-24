#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.gateways.postgresql import PostgreSQLGateway
from manifold.core.query import Query
from manifold.util.log import Log

l = Log('test_postgresql')

def pg_cb(x):
    print "CALLBACK: %r" % x

# Issue a simple query from the psql gateway

c = {'db_user':'postgres', 'db_password':None, 'db_name':'test'}
q = Query(object='test', filters=[['b', ']', 2]], fields=['a', 'b'])
gw = PostgreSQL(router=None, platform='pg', query=q, config=c, user_config=None, user=None)
gw.set_callback(pg_cb)
gw.start()
