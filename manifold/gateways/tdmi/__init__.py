# To avoid naming conflicts when importing 
from __future__                     import absolute_import

import re
from manifold.gateways              import Gateway
from manifold.gateways.postgresql   import PostgreSQLGateway

# Asynchronous support
# http://initd.org/psycopg/docs/advanced.html

class TDMIGateway(PostgreSQLGateway):
    def __init__(self, router, platform, query, config, user_config, user):
        """
        Constructor of TDMIGateway
        Args:
            router: None or a Router instance
            platform: A StringValue. You may pass u'dummy' for example
            query: None or a Query instance
            config: A dictionnary containing information to connect to the postgresql server
                Example :
                    config = {
                        'db_password': None,
                        'db_name': 'tophat',
                        'db_user': 'postgres',
                        'db_host': 'clitos.ipv6.lip6.fr'
                    }
            user_config: An empty dictionnary
            user: None or a User instance
        """
#        ignored_tables = [
#            re.compile("^traceroute"),
#            re.compile("^bgp")
#        ] 
        re_ignored_tables = [re.compile(".*")]

        super(TDMIGateway, self).__init__(router, platform, query, config, user_config, user, re_ignored_tables)
        from manifold.gateways.tdmi.methods   import Traceroute

        # Some table doesn't exists in the PostgreSQL database.
        # Those pseudo-tables are managed by dedicated python objects (see for example methods/*.py).
        # For instance, Traceroute object craft a SQL query involving a stored procedure.
        self.connection = None
        self.METHOD_MAP = {
            "traceroute" : Traceroute
        }

    def start(self):
        """
        Translate self.query into the corresponding SQL command.
        The PostgreSQL's get_sql method is overloaded in order to redirect
        handle queries related to pseudo tables (traceroute, bgp, ...) and craft a
        customized query.
        """
        query = self.query
        if query.object in self.METHOD_MAP.keys():
            # This object is retrieved thanks to a stored procedure
            # See manifold/gateways/tdmi/methods/*
            params = None
            obj = self.METHOD_MAP[query.object](query, db = self)
            sql = obj.get_sql()
            rows = self.selectall(sql, params)
            if obj.repack: rows = obj.repack(rows)
            rows.append(None)
            map(self.send, rows)
        else:
            return super(TDMIGateway, self).start()
