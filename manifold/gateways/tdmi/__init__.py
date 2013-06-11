# To avoid naming conflicts when importing 
from __future__                     import absolute_import

import re
from manifold.core.announce         import Announces
from manifold.core.field            import Field 
from manifold.gateways              import Gateway
from manifold.gateways.postgresql   import PostgreSQLGateway
from manifold.operators             import LAST_RECORD
from manifold.util.type             import accepts, returns 
from manifold.util.log              import Log

# Asynchronous support
# http://initd.org/psycopg/docs/advanced.html

class TDMIGateway(PostgreSQLGateway):

    RE_ID = re.compile("^(.*)_id$")

    def __init__(self, router, platform, query, config, user_config, user):
        """
        Constructor of TDMIGateway
        Args:
            router: None or a Router instance
            platform: A StringValue. You may pass u"dummy" for example
            query: None or a Query instance
            config: A dictionnary containing information to connect to the postgresql server
                Example :
                    config = {
                        "db_password" : None,
                        "db_name"     : "tophat",
                        "db_user"     : "postgres",
                        "db_host"     : "clitos.ipv6.lip6.fr",
                        "db_port"     : 5432
                    }
            user_config: An empty dictionnary
            user: None or a User instance
        """

        # Every tables are private and not exposed to Manifold...
        re_ignored_tables = PostgreSQLGateway.ANY_TABLE 

        # ... excepted the following ones: 
        re_allowed_tables = [
#            re.compile("^agent$"),
            re.compile("^destination$"),
            re.compile("^ip$")#,
#            re.compile("^node$")
        ]

        super(TDMIGateway, self).__init__(router, platform, query, config, user_config, user, re_ignored_tables, re_allowed_tables)
#        self.connection = None

        # Some table doesn't exists in the PostgreSQL database.
        # Those pseudo-tables are managed by dedicated python objects 
        # (see for example manifold/gateways/tdmi/methods/*.py).
        # For instance, Traceroute object crafts a SQL query involving a stored procedure.
        # Since Hops does not exists in the pgsql schema and is only declared to describe
        # the type hops involved in traceroute, we ignore queries related to hops. 

        from manifold.gateways.tdmi.methods   import Traceroute
        from manifold.gateways.tdmi.methods   import Agent 

        self.METHOD_MAP = {
            "traceroute" : Traceroute,
            "agent"      : Agent,
            "hops"       : None 
        }

        # Some Fields do not exists in TDMI's database but are exposed to Manifold
        # (see /usr/share/manifold/metadata/tdmi.h) so we inject the missing Fields
        # in order to get a class consistent with the underlying ontology.
        self.custom_fields = {
            "agent" : [
                Field("const", "string", "platform", None, "Platform annotation, always equal to 'tdmi'")
            ]
        }

        # The following keys are not deduced from the PostgreSQL schema, so we
        # inject them manually since they will be needed to perform joins
        # among the TDMI's tables
        self.custom_keys = {
#            "agent" : [["ip", "platform"]]
        }

        # Map each Manifold name to the corresponding pgsql table/view name
        self.aliases = dict() 

        # Those table are injected in manifold thanks to a dedicated metadata file
        # (here /usr/share/manifold/metadata/tdmi.h). We need to load them in
        # order to support queries involving the traceroute table in a JOIN.
        self.get_metadata()

    def get_pgsql_name(self, manifold_name):
        """
        Translate a name of Manifold object into the appropriate view/table name
        Args:
            manifold_name: the Manifold object name (for example "agent") (String instance)
        Returns:
            The corresponding pgsql name (for instance "view_agent") (String instance)
        """
        if manifold_name in self.aliases.keys():
            return self.aliases[manifold_name]
        return manifold_name

    # TODO move PostgreSQLGateway
    @staticmethod
    def get_colliding_announces(announces1, announces2):
        return frozenset([announce.get_table().get_name() for announce in announces1]) \
             & frozenset([announce.get_table().get_name() for announce in announces2])

    @staticmethod
    def fix_id(table):
        """
        Translate each ID field (named *_id).
        For example, an agent refers to an ip by using an "unsigned ip_id" field
        and an ip record is identified by an "unsigned ip_id" field.
        In the both corresponding Manofld objects, those both fields are translated
        into "const local unsigned ip_id" fields.
        """
        for field in table.get_fields():
            field_name = field.get_name()
            m = TDMIGateway.RE_ID.match(field_name)
            if m:
                new_name = m.group(1)
                if new_name in table.get_field_names():
                    Log.warning("Overwritting this field: %s" % table.get_field(new_name))
                    table.erase_field(new_name)
#                field.local = True
                field.name = new_name
                field.type = new_name 
                field.qualifier = "const"
        return True

    # TODO move PostgreSQLGateway
    @returns(list)
    def get_metadata(self):
        """
        Retrieve metadata related to the TDMIGateway
        """
#        # Fetch metadata from pgsql views. We then remove the "view_" prefix, so
#        # at least, the "foo" class is mapped to the "view_foo" pgsql view.
#        announces_pgsql = super(TDMIGateway, self).make_metadata_from_names(self.get_view_names())
#
#        # Manifold object corresponds to a TDMI view having a differant name
#        re_view_name = re.compile("view_(.*)")
#        for announce in announces_pgsql:
#            table = announce.get_table()
#            pgsql_name = table.get_name()
#            m = re_view_name.match(pgsql_name)
#            if m:
#                table.name = m.group(1)
#                self.aliases[table.get_name()] = pgsql_name
        announces_pgsql = super(TDMIGateway, self).make_metadata_from_names(self.get_table_names())

        # Fetch metadata from .h files
        announces_h = Announces.from_dot_h(self.get_platform(), self.get_gateway_type())

        # Merge metadata (TODO check colliding names)
        s = TDMIGateway.get_colliding_announces(announces_pgsql, announces_h)
        if s:
            Log.warning("TDMIGateway::get_metadata(): colliding announces for table(s): {%s}" % ", ".join(s))
        announces = announces_pgsql + announces_h

        # Inject custom keys and fields
        for announce in announces:
            table = announce.get_table()
            table_name = table.get_name()

            # Translate IDs in Manifold field
#            TDMIGateway.fix_id(table)

            # Inject custom fields in their corresponding announce
            if table_name in self.custom_fields.keys():
                for field in self.custom_fields[table_name]:
                    table.insert_field(field)

            # Inject custom keys in their corresponding announce
            if table_name in self.custom_keys.keys():
                for key in self.custom_keys[table_name]:
                    table.insert_key(key)

        return announces

    def start(self):
        """
        Translate self.query into the corresponding SQL command.
        The PostgreSQL's get_sql method is overloaded in order to redirect
        handle queries related to pseudo tables (traceroute, bgp, ...) and craft a
        customized query.
        """
        query = self.query
        table_name = query.get_from()

        print "-" * 80
        print "%s" % query
        print "-" * 80
        if table_name in self.METHOD_MAP.keys():
            if self.METHOD_MAP[table_name]:
                # See manifold/gateways/tdmi/methods/*
                params = None
                instance = self.METHOD_MAP[table_name](query, db = self)
                sql = instance.get_sql()
                rows = self.selectall(sql, params)

                # Does this object tweak the python dictionnary returned by selectall?
                if instance.repack:
                    # TODO repack_one
                    rows = instance.repack(query, rows)
            else:
                # Dummy object, like hops (hops is declared in tdmi.h) but
                # do not corresponds to any 
                rows = list()

            rows.append(LAST_RECORD)
            map(self.send, rows)
        else:
            # Update FROM clause according to postgresql aliases
            self.query.object = self.get_pgsql_name(table_name)
            return super(TDMIGateway, self).start()
