from manifold.gateways import Gateway

from tdmi.methods import *

METHOD_MAP = {
    'traceroute': Traceroute
}

# Asynchronous support
# http://initd.org/psycopg/docs/advanced.html

class TDMIGateway(PostgreSQLGateway):

    # The difference is that we forge somehow a part of the SQL request... to be
    # analyzed

    def __init__(self):
        self.db = PostgreSQL()

    def execute(self, query, user=None):
        if query.fact_table in METHOD_MAP.keys():
            return METHOD_MAP[query.fact_table](query, db=self.db)

        # Generic query handling
        # What about subqueries ?
        sql = Query(query).sql()
        print "SQL", sql
        return []
        return self.db.selectall(sql)
