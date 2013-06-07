#!/usr/bin/env python
# -*- coding: utf-8 -*-

# psql -U postgres
# CREATE DATABASE test;
#
# psql -U postgres -d test
# CREATE TABLE test (                                                                                   
#     a INTEGER PRIMARY KEY,
#     b INTEGER
# );
# INSERT INTO test (a, b) VALUES (1,1), (2,2), (3,1), (4,4);
# CREATE TABLE test2 (                                                           
#     c INTEGER PRIMARY KEY,
#     d INTEGER REFERENCES test
# );
# INSERT INTO test2 (c, d) VALUES (10,1), (20,2), (30,2);

from manifold.gateways.postgresql import PostgreSQLGateway
from manifold.core.query          import Query
from manifold.util.log            import Log

l = Log('test_postgresql')

def pg_cb(x):
    print "CALLBACK: %r" % x

# Issue a simple query from the psql gateway

c = {'db_user':'postgres', 'db_password':None, 'db_name':'test'}
q = Query(object='test', filters=[['b', ']', 2]], fields=['a', 'b'])
gw = PostgreSQLGateway(router=None, platform='pg', query=q, config=c, user_config=None, user=None)
gw.set_callback(pg_cb)
gw.start()
