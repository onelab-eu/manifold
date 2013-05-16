# To avoid naming conflicts when importing 
from __future__ import absolute_import

from manifold.gateways              import Gateway
from manifold.gateways.postgresql   import PostgreSQLGateway
# Asynchronous support
# http://initd.org/psycopg/docs/advanced.html

class TDMIGateway(PostgreSQLGateway):

   
    # The difference is that we forge somehow a part of the SQL request... to be
    # analyzed

    def __init__(self, router, platform, query, config, user_config, user):
        print "COUCOU"
        super(PostgreSQLGateway, self).__init__(router, platform, query, config, user_config, user)
        from manifold.gateways.tdmi.methods   import Traceroute

        # Some table doesn't in the PostgreSQL database.
        # Those pseudo-tables are in a python object (see for example methods/Traceroute.py).
        # which may for example call a stored procedure with the appropriate parameters.

        self.connection = None
        self.METHOD_MAP = {
            "traceroute": Traceroute
        }
#        self.db = PostgreSQL()

    def execute(self, query, user=None):
        print "Query:\n%s"% query
        if query.object in self.METHOD_MAP.keys():
#            return self.METHOD_MAP[query.object](query, db = self.db)
            return self.METHOD_MAP[query.object](query, db = self)

        # Generic query handling
        # What about subqueries ?
        sql = Query(query).sql()
        print "SQL", sql
#        return []
#        return self.db.selectall(sql)
        return self.selectall(sql)
