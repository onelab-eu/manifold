# To avoid naming conflicts when importing 
from __future__ import absolute_import

from manifold.gateways              import Gateway
from manifold.gateways.postgresql   import PostgreSQLGateway
# Asynchronous support
# http://initd.org/psycopg/docs/advanced.html

class TDMIGateway(PostgreSQLGateway):

   
    # The difference is that we forge somehow a part of the SQL request... to be
    # analyzed

    def __init__(self):
        from tdmi.methods                   import Traceroute

        self.METHOD_MAP = {
            'traceroute': Traceroute
        }
        self.db = PostgreSQL()

    def execute(self, query, user=None):
        if query.object in self.METHOD_MAP.keys():
            return self.METHOD_MAP[query.object](query, db=self.db)

        # Generic query handling
        # What about subqueries ?
        sql = Query(query).sql()
        print "SQL", sql
        return []
        return self.db.selectall(sql)
