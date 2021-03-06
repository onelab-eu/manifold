#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions for interacting with a PostgreSQL server 
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

# For tests:
# $ psql -U postgres
# postgres=# CREATE DATABASE test;
# $ psql -U postgres -d test
# CREATE AGGREGATE array_accum (anyelement)
# (
#     sfunc = array_append,
#     stype = anyarray,
#     initcond = '{}'
# );
# CREATE SEQUENCE test_seq START 1;
# CREATE TABLE test (
#     test_id integer PRIMARY KEY DEFAULT nextval('test_seq'),
#     name text
# );
# CREATE TABLE trans (
#     trans_id integer PRIMARY KEY REFERENCES test,
#     en_name text 
# );
#
# /etc/postgresql/9.1/main/pg_hba.conf
# local all all trust

# NOTE: A table should have a primary key, and references to other tables
#
# Single table queries:
#
# INSERT INTO test (test_id, name) VALUES (1, 'un');
# manifold-add-platform psql psql postgresql none '{"db_user": "postgres", "db_name": "test"}' 0 
# manifold-enable-platform psql
# manifold-shell -z local
# manifold>>> SELECT test_id, name FROM test
# manifold>>> INSERT INTO test SET id=2, name='deux'
# manifold>>> UPDATE test SET name="Deux" WHERE test_id == 2
# manifold>>> DELETE FROM test WHERE test_id == 2
#
# Multiple table queries:

# insert into test SET name = "quatre", en_name = "four" SELECT name, en_name
#   [{'en_name': u'four', 'name': u'quatre'}]
# manifold>>> select name, en_name from test where name == "quatre"
#   [{'en_name': u'four', 'name': u'quatre'}]
#
# BUGS:
#
# [ ] Infinite loop when doing a SELECT id...
#
#
# SFA related tests
#
# CREATE TABLE slice (
#     slice_urn text PRIMARY KEY,
#     authority_hrn text
# );
# CREATE TABLE slice_ext (
#     slice_hrn text PRIMARY KEY REFERENCES slice,
#     description text
# );
#



# Some code borrowed from MyPLC PostgreSQL code
import psycopg2
import psycopg2.extensions
import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
# UNICODEARRAY not exported yet
psycopg2.extensions.register_type(psycopg2._psycopg.UNICODEARRAY)

import re, datetime, pgdb
from itertools                import izip
from uuid                     import uuid4
from types                    import StringTypes, GeneratorType, NoneType, IntType, LongType, FloatType, ListType, TupleType
from pprint                   import pformat
from manifold.gateways        import Gateway
from manifold.core.announce   import Announce, Announces
from manifold.core.table      import Table
from manifold.core.field      import Field
from manifold.core.query      import ACTION_CREATE, ACTION_GET, ACTION_UPDATE, ACTION_DELETE
from manifold.core.record     import Record, Records, LastRecord
from manifold.util.log        import Log
from manifold.util.predicate  import and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg, contains
from manifold.util.type       import accepts, returns

class PostgreSQLGateway(Gateway):
    __gateway_name__ = 'postgresql'

    DEFAULT_DB_NAME     = "postgres"
    DEFAULT_DB_HOST     = "localhost"
    DEFAULT_DB_PORT     = 5432
    DEFAULT_DB_USER     = "postgres"
    DEFAULT_DB_PASSWORD = ""

    # Note: escaping %(table_name)s with double quotes is required
    # to manage "user" table. We can query "user" but not user
    # (which concerns the internal pgsql table)
    # http://stackoverflow.com/questions/4350874/unable-to-use-table-named-user-in-postgresql-hibernate

    SQL_SELECT_STR = """
    SELECT %(fields)s
        FROM "%(table_name)s"
        %(where)s
        ;
    """

    SQL_INSERT_STR = """
    INSERT INTO "%(table_name)s"
        (%(param_fields)s)
        VALUES (%(param_values)s)
        %(returning)s
        ;
    """

    SQL_DELETE_STR = """
    DELETE FROM "%(table_name)s"
        %(where)s
        ;
    """

    SQL_UPDATE_STR = """
    UPDATE "%(table_name)s"
        SET %(params)s
        %(where)s
        ;
    """

    SQL_OPERATORS = {
        eq: "="
    }

    SQL_CREATE_DATABASE = """
        CREATE DATABASE %(database_name)s;
    """

    SQL_CHECK_TABLE_EXISTS = """
    SELECT EXISTS(
        SELECT 1 
        FROM   pg_catalog.pg_class c
        JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE  n.nspname = 'public'
        AND    c.relname = %(table_name)s
    );
    """
    # 'public' was 'schema_name'

    SQL_CREATE_TABLE = """
        CREATE TABLE %(table_name)s (
            %(columns)s
        );
    """

    #-------------------------------------------------------------------------------
    # Metadata 
    #-------------------------------------------------------------------------------

    # SGBD

    SQL_SGBD_DBNAMES = """
    SELECT    datname
        FROM  pg_database
        WHERE datistemplate = false;
    """

    # Database

    SQL_DB_TABLE_NAMES = """
    SELECT    table_name 
        FROM  information_schema.tables 
        WHERE table_schema  = 'public'
          AND table_type    = 'BASE TABLE'
          AND table_catalog = '%(db_name)s'
    """

    SQL_DB_VIEW_NAMES = """
    SELECT    table_name
        FROM  information_schema.views
        WHERE table_schema  = ANY(current_schemas(false))
          AND table_catalog = '%(db_name)s'
    """

    # Table / view

    SQL_TABLE_FIELDS = """
    SELECT    column_name, data_type, is_updatable, is_nullable
        FROM  information_schema.columns
        WHERE table_name = %(table_name)s ORDER BY ordinal_position
    """

    SQL_TABLE_KEYS = """
    SELECT       tc.table_name                        AS table_name,
                 array_accum(kcu.column_name::text)   AS column_names
        FROM     information_schema.table_constraints AS tc   
            JOIN information_schema.key_column_usage  AS kcu ON tc.constraint_name = kcu.constraint_name
        WHERE    constraint_type = 'PRIMARY KEY' AND tc.table_name = %(table_name)s
        GROUP BY tc.table_name;
    """

    # Full request:
    #   SELECT
    #     tc.constraint_name,
    #     tc.table_name,
    #     kcu.column_name, 
    #     ccu.table_name  AS foreign_table_name,
    #     ccu.column_name AS foreign_column_name 
    #   FROM [...]

    SQL_TABLE_FOREIGN_KEYS = """
    SELECT       kcu.column_name, ccu.table_name AS foreign_table_name
        FROM     information_schema.table_constraints       AS tc 
            JOIN information_schema.key_column_usage        AS kcu ON tc.constraint_name  = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
        WHERE    constraint_type = 'FOREIGN KEY'
          AND    tc.table_name = %(table_name)s;
    """

    SQL_TABLE_COMMENT = """
    SELECT    relname, obj_description(oid) AS description
        FROM  pg_class
        WHERE (relkind = 'r' OR relkind = 'v')
          AND relname = %(table_name)s
    """

    SQL_TABLE_COLUMNS_COMMENT = """
    SELECT
        a.attname                             AS column_name,
        col_description(a.attrelid, a.attnum) AS description 
    FROM     pg_catalog.pg_attribute    AS a
        JOIN pg_catalog.pg_class        AS c ON a.attrelid = c.oid
    WHERE   c.relname = %(table_name)s
        AND a.attnum > 0
        AND a.attisdropped IS FALSE
        AND pg_catalog.pg_table_is_visible(c.oid);
    """

    ANY_TABLE  = [re.compile(".*")]
    NONE_TABLE = []

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, router, platform, query, config, user_config, user, re_ignored_tables = NONE_TABLE, re_allowed_tables = ANY_TABLE):
        """
        Construct a PostgreSQLGateway instance
        Args:
            re_ignored_tables: A list of re instances filtering tables that must be
                not processed by PostgreSQLGateway. For instance you could filter tables
                not exposed to Manifold. You may also pass:
                - ANY_TABLE:  every table are ignored if not explicitly accepted
                - NONE_TABLE: no table is filtered
            re_allowed_tables: A list of re instances allowing tables. This supersedes
                table filtered by re_ignored_tables regular expressions. You may
                also pass
        """
        super(PostgreSQLGateway, self).__init__(router, platform, query, config, user_config, user)
        self.connection = None
        self.cursor = None

        # The table matching those regular expressions are ignored...
        self.re_ignored_tables = re_ignored_tables

        # ... excepted the ones explicitly allowed
        self.re_allowed_tables = re_allowed_tables

        # This { String : String } dictionnary maps each Manifold object
        # name with its corresponding pgsql table/view name.
        # If the both names match, you do not need to provide alias.
        #
        # Example:
        #
        #   self.table_aliases = {
        #       "my_object_name" : "my_table_name",
        #       "foo"            : "view_foo"
        #   }
        self.table_aliases = dict() 

        # This {String : list(Field)} dictionnary is used to inject
        # additional Fields in the Manifold object which correspond
        # to columns not declared in the pgsql schema. The Gateway
        # is supposed to inject the appropriate value in the returned
        # records.
        #
        # Example:
        #
        #   self.custom_fields = {
        #       "agent" : [
        #           Field("const", "string", "platform", None, "Platform annotation, always equal to 'tdmi'")
        #       ]
        #   }
        self.custom_fields = dict() 

        # This {String : list(list(String))} dictionnary is used to inject
        # additional Keys (list of field names) in the Manifold object not
        # declared in the pgsql schema. These custom keys may involve custom
        # fields.
        #
        # Example:
        #
        #   self.custom_keys = {
        #       "agent" : [["ip", "platform"]]
        #   }
        self.custom_keys = dict()

        self.metadata = None

    #---------------------------------------------------------------------------
    # (Internal usage)
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def get_pgsql_name(self, manifold_name):
        """
        (Internal usage)
        Translate the name of Manifold object into the appropriate view/table name
        Args:
            manifold_name: the Manifold object name (for example "agent") (String instance)
        Returns:
            The corresponding pgsql name (for instance "view_agent") (String instance).
            If not found, returns the value stored in the manifold_name parameter.
        """
        if manifold_name in self.table_aliases.keys():
            return self.table_aliases[manifold_name]
        return manifold_name

    #---------------------------------------------------------------------------
    # Accessors 
    #---------------------------------------------------------------------------

    @returns(list)
    def get_databases(self):
        """
        Retrieve the database names stored in postgresql
        Returns:
            A list of StringTypes containing the database names (excepted postgres).
        """
        return [x["datname"] for x in self.selectall(PostgreSQLGateway.SQL_SGBD_DBNAMES) if x["datname"] != "postgres"]

    @returns(GeneratorType)
    def _get_generator(self, sql_query):
        """
        (Internal usage)
        Build a generator allowing to iterate on the first
        field of a set of queried records
        Args:
            sql_query: A SQL query passed to PostgreSQL (String instance)
        Returns:
            The correspnding generator instance
        """
        cursor = self.get_cursor()
        cursor.execute(sql_query)
        return (record[0] for record in cursor.fetchall())

    @returns(GeneratorType)
    def get_view_names(self):
        """
        Retrieve the view names stored in the current database
        Returns:
            A generator allowing to iterate on each view names (String instance)
        """
        return self._get_generator(PostgreSQLGateway.SQL_DB_VIEW_NAMES % self.config)

    @returns(GeneratorType)
    def get_table_names(self):
        """
        Retrieve the table names stored in the current database
        Returns:
            A generator allowing to iterate on each table names (String instance)
        """
        return self._get_generator(PostgreSQLGateway.SQL_DB_TABLE_NAMES % self.config)

    # TODO this could be moved into Gateway to implement "Access List"
    @returns(bool)
    def is_ignored_table(self, table_name):
        """
        Check whether a Table must be processed by this PostgreSQLGateway
        Args:
            table_name: A StringValue corresponding to the name of the Table
        Returns:
            A bool equal to True iif this table must be ignored
        """
        for re_ignored_table in self.re_ignored_tables:
            if re_ignored_table.match(table_name):
                for re_allowed_table in self.re_allowed_tables:
                    if re_allowed_table.match(table_name):
                        return False
                return True
        return False

    def get_cursor(self, cursor_factory = None):
        """
        Retrieve the cursor used to interact with the PostgreSQL server.
        Args:
            cursor_factory: see http://initd.org/psycopg/docs/extras.html
        Returns:
            The cursor used to interact with the PostgreSQL server.
        """
        return self.cursor if self.cursor else self.connect(cursor_factory = psycopg2.extras.NamedTupleCursor)

    #---------------------------------------------------------------------------
    # Connection 
    #---------------------------------------------------------------------------

    @returns(dict)
    def make_psycopg2_config(self):
        """
        Prepare the dictionnary needed to prepare a PostgreSQL connection by
        using psycopg2 based on the self.config dictionnary
        Returns:
            The corresponding psycopg2-compliant dictionnary
        """
        return {
            "user"     : self.config.get("db_user",     self.DEFAULT_DB_USER),
            "password" : self.config.get("db_password", self.DEFAULT_DB_PASSWORD),
            "database" : self.config.get("db_name",     self.DEFAULT_DB_NAME),
            "host"     : self.config.get("db_host",     self.DEFAULT_DB_HOST),
            "port"     : self.config.get("db_port",     self.DEFAULT_DB_PORT),
        }

    @returns(bool)
    def connect_unix(self):
        """
        (Internal usage)
        Establish a UNIX connection with the PostgreSQL server
        Initialize self.connection
        Params:
            cursor_factory: see http://initd.org/psycopg/docs/extras.html
        Returns:
            True iif successful.
        """
        try:
            psycopg2_cfg = self.make_psycopg2_config()
            del psycopg2_cfg["host"]
            del psycopg2_cfg["port"]
            self.connection = psycopg2.connect(**psycopg2_cfg)
            return True
        except psycopg2.OperationalError:
            return False

    @returns(bool)
    def connect_tcp(self):
        """
        (Internal usage)
        Establish a TCP connection with the PostgreSQL server
        Initialize self.connection
        Params:
            cursor_factory: see http://initd.org/psycopg/docs/extras.html
        Returns:
            True iif successful.
        """
        psycopg2_cfg = self.make_psycopg2_config()
        self.connection = psycopg2.connect(**psycopg2_cfg)
        self.connection.set_client_encoding("UNICODE")
        return True

    def connect(self, cursor_factory = None): #psycopg2.extras.NamedTupleCursor
        """
        (Internal usage)
        Establish a connection with the PostgreSQL server
        Initialize self.connection
        Params:
            cursor_factory: see http://initd.org/psycopg/docs/extras.html
        Raises:
            RuntimeError: if the connection cannot be established
        Returns:
            The corresponding cursor
        """
        connection_ok = (self.connection != None)
        if not connection_ok:
            connection_ok = self.connect_unix()
        if not connection_ok: 
            connection_ok = self.connect_tcp()
        if not connection_ok: 
            raise RuntimeError("Cannot connect to PostgreSQL server")

        # Needed to manage properly cascading execute(), maybe OBSOLETE 
        self.rowcount    = None
        self.description = None
        self.lastrowid   = None

        if cursor_factory:
            return self.connection.cursor(cursor_factory = cursor_factory)
        else:
            return self.connection.cursor()

    def close(self):
        """
        Close connection established with the PostgreSQL server (if any)
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None

#OBSOLETE|    @staticmethod
#OBSOLETE|    def param(self, name, value):
#OBSOLETE|        if isinstance(value, NoneType):
#OBSOLETE|            # None is converted to the unquoted string NULL
#OBSOLETE|            conversion = "s"
#OBSOLETE|        elif isinstance(value, bool):
#OBSOLETE|            # True and False are also converted to unquoted strings
#OBSOLETE|            conversion = "s"
#OBSOLETE|        elif isinstance(value, float):
#OBSOLETE|            conversion = "f"
#OBSOLETE|        elif not isinstance(value, StringTypes):
#OBSOLETE|            conversion = "d"
#OBSOLETE|        else:
#OBSOLETE|            conversion = "s"
#OBSOLETE|
#OBSOLETE|        return "%(" + name + ")" + conversion
#OBSOLETE|
#OBSOLETE|    def begin_work(self):
#OBSOLETE|        # Implicit in pgdb.connect()
#OBSOLETE|        pass

    def commit(self):
        """
        Commit a sequence of SQL commands
        """
        self.connection.commit()

    def rollback(self):
        """
        Cancel a sequence of SQL commands 
        """
        self.connection.rollback()

    #---------------------------------------------------------------------------
    # Overloaded methods 
    #---------------------------------------------------------------------------

    def forward(self, query, deferred = False, execute = True, user = None):
        """
        Args
            query: A Query instance that must be processed by this PostgreSQLGateway
            deferred: A boolean
            execute: A boolean which must be set to True if the query must be run.
            user: A User instance or None
        """
        self.query = query
        self.start()

    def start(self):
        """
        Fetch records stored in the postgresql database according to self.query
        """
        sql = PostgreSQLGateway.to_sql(self.query)
        Log.tmp(sql)
        if self.query.get_action() in [ACTION_GET, ACTION_CREATE]:
            rows = self.selectall(sql)
        else:
            count = self.do(sql)
            if self.query.get_select():
                select_query = self.query.copy()
                select_query.action = ACTION_GET
                if self.query.get_action() == ACTION_DELETE:
                    select_query.filter_by(None)
                select_sql = PostgreSQLGateway.to_sql(select_query)
                rows = self.selectall(select_sql)
            else:
                rows = list()

        map(self.send, Records(rows))
        self.send(LastRecord())
       
    @staticmethod
    def get_colliding_announces(announces1, announces2):
        return frozenset([announce.get_table().get_name() for announce in announces1]) \
             & frozenset([announce.get_table().get_name() for announce in announces2])

    @returns(list)
    def merge_announces(self, announces_pgsql, announces_h):
        # Merge metadata
        s = PostgreSQLGateway.get_colliding_announces(announces_pgsql, announces_h)
        if s:
            Log.warning("merge_announces: colliding announces for table(s): {%s}" % ", ".join(s))

        announces  = list()
        announces.extend(announces_h)
        table_names = [announce.get_table().get_name() for announce in announces_h]
        for announce_pgsql in announces_pgsql:
            table_name = announce_pgsql.get_table().get_name()
            if table_name not in table_names:
                announces.append(announces_pgsql)
                table_names.append(table_name)
        
        return announces

    @returns(list)
    def tweak_announces(self, announces_pgsql):
        # Inject custom keys and fields
        for announce in announces_pgsql:
            table = announce.get_table()
            table_name = table.get_name()

            # Inject custom fields in their corresponding announce
            if table_name in self.custom_fields.keys():
                for field in self.custom_fields[table_name]:
                    table.insert_field(field)

            # Inject custom keys in their corresponding announce
            if table_name in self.custom_keys.keys():
                for key in self.custom_keys[table_name]:
                    table.insert_key(key)

        return announces_pgsql

    def make_metadata(self):
        """
        Prepare metadata (e.g. Tables encapsulated in Announces instances) related
        to this Gateway. Metadata are in the generic case retrieved both by
        inspecting the pgsql schema and the .h file related to this Gateway.
        """
        # Import metadata from pgsql schema.
        # By default, we only fetch tables and we ignore views.
        ########
        announces_pgsql = self.make_metadata_from_names(self.get_table_names())
        if not announces_pgsql:
            Log.warning("Cannot find metadata for platform %s: %s" % (self.platform, e))
        else:
            Log.info("Tables imported from pgsql schema: %s" % [announce.get_table() for announce in announces_pgsql])
        ###
        """
        class table {
            string comment;
            field  fields[];
            key    keys[];
        };

        class field {
            string comment;
            bool   is_const;
            bool   is_array;
            string type;
        };

        class key {
            table table;    /**< BACKWARD_1N */
            field fields[];
        };
        """
        # 1) router::boot:
        #      for each gateway:
        #        if gateway.type == postgresql:
        #        router.instantiate_gateway(gateway)
        #    PostgreSQLGateway::__init__():
        #      self.router.instantiate_gateway(platform = postgresql_metadata, config = config)
        # 2) d = self.router.forward(Query.get('object').select([name, field.name, field.type, field.comment, key])
        #   see core/interface.py:180
        # 3) announces_pgsql = Announce.from_dict(d)
        # In this gateway inject field info in table info
        #########

        # Fetch metadata from .h files (if any)
        announces_h = Announces.from_dot_h(self.get_platform(), self.get_gateway_type())
        Log.info("Tables imported from .h schema: %s" % [announce.get_table() for announce in announces_h])

        # Return the resulting announces
        return self.merge_announces(announces_pgsql, announces_h) if announces_h else announces_pgsql

    def get_metadata(self):
        if not self.metadata:
            self.metadata = self.make_metadata()
        return self.metadata

    def do(self, query, params = None):
        cursor = self.execute(query, params)
        cursor.close()
        return self.rowcount

#OBSOLETE|    def next_id(self, table_name, primary_key):
#OBSOLETE|        sequence = "%(table_name)s_%(primary_key)s_seq" % locals()  
#OBSOLETE|        sql = "SELECT nextval('%(sequence)s')" % locals()
#OBSOLETE|        rows = self.selectall(sql, hashref = False)
#OBSOLETE|        if rows: 
#OBSOLETE|            return rows[0][0]
#OBSOLETE|            
#OBSOLETE|        return None 
#OBSOLETE|
#OBSOLETE|    def last_insert_id(self, table_name, primary_key):
#OBSOLETE|        if isinstance(self.lastrowid, int):
#OBSOLETE|            sql = "SELECT %s FROM %s WHERE oid = %d" % \
#OBSOLETE|                  (primary_key, table_name, self.lastrowid)
#OBSOLETE|            rows = self.selectall(sql, hashref = False)
#OBSOLETE|            if rows:
#OBSOLETE|                return rows[0][0]
#OBSOLETE|
#OBSOLETE|        return None

    def execute(self, query, params = None, cursor_factory = None):
        """
        Execute a SQL query on PostgreSQL 
        Args:
            query: a String containing a SQL query 
            params: a dictionnary or None if unused 
            cursor_factory: see http://initd.org/psycopg/docs/extras.html
        Returns:
            The corresponding cursor
        """
        # modified for psycopg2-2.0.7 
        # executemany is undefined for SELECT's
        # see http://www.python.org/dev/peps/pep-0249/
        # accepts either None, a single dict, a tuple of single dict - in which case it execute's
        # or a tuple of several dicts, in which case it executemany's

        Log.debug(query)

        cursor = self.connect(cursor_factory)
        try:
            # psycopg2 requires %()s format for all parameters,
            # regardless of type.
            # this needs to be done carefully though as with pattern-based filters
            # we might have percents embedded in the query
            # so e.g. GetPersons({"email":"*fake*"}) was resulting in .. LIKE "%sake%"
            if psycopg2:
                query = re.sub(r"(%\([^)]*\)|%)[df]", r"\1s", query)
            # rewrite wildcards set by Filter.py as "***" into "%"
            query = query.replace("***", "%")

            if not params:
                cursor.execute(query)
            elif isinstance(params, StringValue):
                cursor.execute(query, params)
            elif isinstance(params, dict):
                cursor.execute(query, params)
            elif isinstance(params, tuple) and len(params) == 1:
                cursor.execute(query, params[0])
            else:
                param_seq = params
                cursor.executemany(query, param_seq)
            (self.rowcount, self.description, self.lastrowid) = \
                            (cursor.rowcount, cursor.description, cursor.lastrowid)
        except Exception, e:
            try:
                self.rollback()
            except:
                pass
            uuid = uuid4() #commands.getoutput("uuidgen")
            Log.debug("Database error %s:" % uuid)
            Log.debug(e)
            Log.debug("Query:")
            Log.debug(query)
            Log.debug("Params:")
            Log.debug(pformat(params))
            msg = str(e).rstrip() # jordan
            raise Exception(self.make_error_message(msg, uuid))

        return cursor

    # see instead: psycopg2.extras.NamedTupleCursor
    def selectall(self, query, params = None, hashref = True, key_field = None):
        """
        Return each row as a dictionary keyed on field name (like DBI
        selectrow_hashref()). If key_field is specified, return rows
        as a dictionary keyed on the specified field (like DBI
        selectall_hashref()).

        If params is specified, the specified parameters will be bound
        to the query.
        """
        cursor = self.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        self.commit()
        if hashref or key_field is not None:
            # Return each row as a dictionary keyed on field name
            # (like DBI selectrow_hashref()).
            labels = [column[0] for column in self.description]
            rows = [dict(zip(labels, row)) for row in rows]

        if key_field is not None and key_field in labels:
            # Return rows as a dictionary keyed on the specified field
            # (like DBI selectall_hashref()).
            return dict([(row[key_field], row) for row in rows])
        else:
            return rows

#OBSOLETE|    def fields(self, table, notnull = None, hasdef = None):
#OBSOLETE|        """
#OBSOLETE|        Return the names of the fields of the specified table.
#OBSOLETE|        """
#OBSOLETE|        if hasattr(self, "fields_cache"):
#OBSOLETE|            if self.fields_cache.has_key((table, notnull, hasdef)):
#OBSOLETE|                return self.fields_cache[(table, notnull, hasdef)]
#OBSOLETE|        else:
#OBSOLETE|            self.fields_cache = {}
#OBSOLETE|
#OBSOLETE|        sql = "SELECT attname FROM pg_attribute, pg_class" \
#OBSOLETE|              " WHERE pg_class.oid = attrelid" \
#OBSOLETE|              " AND attnum > 0 AND relname = %(table)s"
#OBSOLETE|
#OBSOLETE|        if notnull is not None:
#OBSOLETE|            sql += " AND attnotnull is %(notnull)s"
#OBSOLETE|
#OBSOLETE|        if hasdef is not None:
#OBSOLETE|            sql += " AND atthasdef is %(hasdef)s"
#OBSOLETE|
#OBSOLETE|        rows = self.selectall(sql, locals(), hashref = False)
#OBSOLETE|        self.fields_cache[(table, notnull, hasdef)] = [row[0] for row in rows]
#OBSOLETE|
#OBSOLETE|        return self.fields_cache[(table, notnull, hasdef)]

    #---------------------------------------------------------------------------
    # Query to SQL 
    #---------------------------------------------------------------------------

    @staticmethod
    def _to_sql_value(x):
        """
        (Internal usage)
        Translate a python value into a PostgreSQL value.
        See quote()
        Args:
            x: The python value
        Raises:
            ValueError: if x cannot be translated
        Returns:
            A String containing a quoted version of the specified value
            or a numerical value.
        """
        if isinstance(x, datetime.datetime):# DateTimeType):
            x = str(x)
        elif isinstance(x, unicode):
            x = x.encode("utf-8")

        if isinstance(x, StringTypes):
            x = "'%s'" % str(x).replace("\\", "\\\\").replace("'", "''")
        elif isinstance(x, (IntType, LongType, FloatType)):
            x = str(x)
        elif x is None:
            x = "NULL"
        elif isinstance(x, (ListType, TupleType)):
            x = "(%s)" % ",".join(map(lambda x: str(PostgreSQLGateway._to_sql_value(x)), x))
        elif hasattr(x, "__pg_repr__"):
            x = x.__pg_repr__()
        else:
            raise ValueError("_to_sql_value: Cannot handle type %s" % type(x))
        return x

    @staticmethod
    def quote(value):
        """
        Translate a python value into a PostgreSQL value.
        See quote()
        Args:
            x: The python value
        Raises:
            ValueError: if x cannot be translated
        Returns:
            A String containing a quoted version of the specified value
            or a numerical value.
        """
        # The pgdb._quote function is good enough for general SQL
        # quoting, except for array types.
        if isinstance(value, (list, tuple, set, frozenset)):
            return "ARRAY[%s]" % ", ".join(map(PostgreSQLGateway.quote, value))
        else:
            return PostgreSQLGateway._to_sql_value(value)

    @staticmethod
    #@accepts(Predicate)
    @returns(StringTypes)
    def _to_sql_where_elt(predicate):
        """
        (Internal usage)
        Translate a Predicate in the corresponding SQL clause
        Args:
            predicate: A Predicate instance
        Returns:
            The String containing the corresponding SQL clause
        """
        # NOTE : & | operator on list, tuple, set: if not make one
        # NOTE : in MyPLC we could have several modifiers on a field

        field, op_, value = predicate.get_tuple()
        op = None

        if isinstance(value, (list, tuple, set, frozenset)):
            # handling filters like '~slice_id':[]
            # this should return true, as it's the opposite of 'slice_id':[] which is false
            # prior to this fix, 'slice_id':[] would have returned ``slice_id IN (NULL) '' which is unknown 
            # so it worked by coincidence, but the negation '~slice_ids':[] would return false too
            if not value or len(list(value)) == 0:
                if op_ in [and_, or_]:
                    operator = eq
                    value = "'{}'"
                else:
                    field = ""
                    operator = ""
                    value = "FALSE"
            else:
                if isinstance(field, (list, tuple, set, frozenset)):
                    and_clauses = []
                    for value_elt in value:
                        value_elt = map(PostgreSQLGateway._to_sql_value, value_elt)
                        predicate_list = ["%s = %s" % (f, ve) for f, ve in izip(field,value_elt)]
                        and_clauses.append(" AND ".join(predicate_list))
                    field = ""
                    op    = ""
                    value = " OR ".join(and_clauses)
                else:
                    value = map(PostgreSQLGateway.quote, value)
                    if op_ == and_:
                        op = "@>"
                        value = "ARRAY[%s]" % ", ".join(value)
                    elif op == or_:
                        op = "&&"
                        value = "ARRAY[%s]" % ", ".join(value)
                    else:
                        op = "IN"
                        value = "(%s)" % ", ".join(value)
        else:
            if value is None:
                op = "IS"
                value = "NULL"
            elif isinstance(value, StringTypes) and \
                    (value.find("*") > -1 or value.find("%") > -1):
                op = "LIKE"
                # insert *** in pattern instead of either * or %
                # we dont use % as requests are likely to %-expansion later on
                # actual replacement to % done in PostgreSQL.py
                value = value.replace ("*", "***")
                value = value.replace ("%", "***")
                value = str(PostgreSQLGateway.quote(value))
            else:
                if op_ == eq:
                    op = "="
                elif op_ == lt:
                    op = "<"
                elif op_ == gt:
                    op = ">"
                elif op_ == le:
                    op = "<="
                elif op_ == ge:
                    op = ">="
                else:
                    Log.error("_to_sql_where_elt: invalid operator: op_ = %s" % op_)

                if isinstance(value, StringTypes) and value[-2:] != "()":
                    # This is a string value and we're not calling a pgsql function
                    # having no parameter (for instance NOW())
                    value = str(PostgreSQLGateway.quote(value))
                elif isinstance(value, datetime.datetime):
                    value = str(PostgreSQLGateway.quote(str(value)))

        clause = "%s %s %s" % (
            "\"%s\"" % field if field else "",
            "%s"     % op    if op    else "",
            value
        )

        if op_ == neg:
            clause = " ( NOT %s ) " % (clause)

        return clause

    @staticmethod
    @returns(StringTypes)
    def to_sql_where(predicates):
        """
        Translate a set of Predicate instances in the corresponding SQL string
        Args:
            predicates: A set of Predicate instances (list, set, frozenset, generator, ...)
        Returns:
            A String containing the corresponding SQL WHERE clause.
            This String is equal to "" if filters is empty
        """
        # NOTE : How to handle complex clauses
        return " AND ".join([PostgreSQLGateway._to_sql_where_elt(predicate) for predicate in predicates])

    @staticmethod
    @returns(StringTypes)
    def get_ts(ts):
        """
        Translate a python timestamp a SQL compliant string
        Args:
            ts: A StringType or a date or datetime instance containing a timestamp.
                String instances containing a timestamp must respect the following
                format:
                    '%Y-%m-%d %H:%M:%S'
                You may also pass "latest" or None which means "now"
        Returns:
            The corresponding string
        Raises:
            ValueError: if parameter ts is not valid
        """
        try:
            if isinstance(ts, StringTypes):
                if ts == "latest" or ts == None:
                    ret = "NULL"
                else:
                    ret = "'%s'" % ts
            elif isinstance(ts, datetime.datetime):
                ret = ts.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(ts, datetime.date):
                ret = ts.strftime("%Y-%m-%d 00:00:00")
        except:
            raise ValueError("Invalid parameter: ts = %s" % ts)
        return ret

    @staticmethod
    @returns(tuple)
    def get_ts_bounds(ts):
        """
        Convert a timestamp or a pair of timestamp into the corresponding
        SQL compliant value(s) 
        Args:
            ts: The input timestamp(s). Supported types:
                ts
                [ts_min, ts_max]
                (ts_min, ts_max)
        Raises:
            ValueError: if parameter ts is not valid
        Returns:
            The corresponding (ts_min, ts_max) tuple in SQL format
        """
        try:
            if isinstance(ts, StringTypes):
                ts_min = ts_max = ts
            elif type(ts) is tuple:
                (ts_min, ts_max) = ts
            elif type(ts) is list:
                [ts_min, ts_max] = ts
        except:
            raise ValueError("Invalid parameter: ts = %s" % ts)
        return (PostgreSQLGateway.get_ts(ts_min), PostgreSQLGateway.get_ts(ts_max))

    @staticmethod
    def to_sql_params_tuple(params):
        """
        Returns the parameters keys in the field order
        """
        field_list = list()
        value_list = list()
        for k, v in params.items():
            field_list.append(k)

            if v is None:
                v = 'NULL'
            elif isinstance(v, StringTypes):
                v = str(PostgreSQLGateway.quote(v))
            else:
                v = str(v)
            value_list.append(v)
        return ", ".join(field_list), ", ".join(value_list)

    @staticmethod
    def to_sql_params(params):
        param_list = list()
        for k, v in params.items():
            if v is None:
                v = 'NULL'
            elif isinstance(v, StringTypes):
                v = str(PostgreSQLGateway.quote(v))
            else:
                v = str(v)
            param_list.append("%s = %s" % (k, v))
        return ", ".join(param_list)

    @staticmethod
    def to_sql(query):
        """
        Translate self.query in the corresponding postgresql command
        Args:
            query: A Query instance
        Returns:
            A String containing a postgresql command 
        """
        where = PostgreSQLGateway.to_sql_where(query.get_where())
        action = query.get_action()
        params = query.get_params()
        fields = query.get_select()
        param_fields, param_values = PostgreSQLGateway.to_sql_params_tuple(params)
        sql_params = {
            # all
            "table_name": query.get_from(),
            "fields": ", ".join(fields),
            # SELECT, INSERT
            "param_fields"    : param_fields,
            # INSERT
            "param_values"    : param_values,
            # SELECT, DELETE, UPDATE
            "where"     : "WHERE %s" % where if where else "",
            # UPDATE
            "params"    : PostgreSQLGateway.to_sql_params(params),
            "returning" : "RETURNING %s" % ", ".join(fields) if fields else "",
        }

        if action == ACTION_CREATE:
            sql = PostgreSQLGateway.SQL_INSERT_STR % sql_params
        elif action == ACTION_GET:
            sql = PostgreSQLGateway.SQL_SELECT_STR % sql_params
        elif action == ACTION_UPDATE:
            sql = PostgreSQLGateway.SQL_UPDATE_STR % sql_params
        elif action == ACTION_DELETE:
            sql = PostgreSQLGateway.SQL_DELETE_STR % sql_params
        else:
            raise Exception, "Not implemented"

        return sql

    @staticmethod
    @returns(StringTypes)
    def to_manifold_type(sql_type):
        """
        Translate a SQL type in its corresponding Manifold type
        Args:
            sql_type: A standard SQL type (for instance boolean, integer, ARRAY, etc.)
        Returns;
            The corresponding Manifold type
        """
        sql_type = sql_type.lower()
        re_timestamp = re.compile("timestamp")
        if sql_type == "integer":
            return "int"
        elif sql_type == "boolean":
            return "bool"
        elif sql_type == "array":
            # TODO we need to infer the right type 
            return "string"
        elif sql_type == "real":
            return "double"
        elif sql_type in ["inet", "cidr", "text", "interval"]:
            return sql_type
        elif re_timestamp.match(sql_type):
            return "timestamp"
        else:
            print "to_manifold_type: %r is not supported" % sql_type

    #---------------------------------------------------------------------------
    # Metadata 
    #---------------------------------------------------------------------------

    def get_table_infos(self, table_name, sql):
        cursor = self.get_cursor()
        cursor.execute(sql, {"table_name": table_name})
        return cursor.fetchall()

    @returns(dict)
    def get_tables_comment(self):
        """
        Retrieve for each table/view the corresponding comment.
        Those comments are set thanks to:
            COMMENT ON TABLE my_table IS 'My table description';
            COMMENT ON VIEW  my_view  IS 'My view description';

        Returns:
            A dictionnary {String : String} which map a table_name to
            its corresponding description.
        """
        cursor = self.get_cursor()
        cursor.execute(PostgreSQLGateway.SQL_TABLE_COMMENT, {"table_name": table_name})
        comments = cursor.fetchall()
        return {comment.relname : comment.description for comment in comments}

    @returns(dict)
    def get_fields_comment(self, table_name):
        """
        Retrieve for each table/view the corresponding comment.
        Those comments are set thanks to:
            COMMENT ON COLUMN my_table.my_field IS 'My field description';

        Params:
            table_name: A String instance containing the name of a table
                belonging to the current database.
        Returns:
            A dictionnary {String : String} which map a field_name to
            its corresponding description.
        """
        cursor = self.get_cursor()
        cursor.execute(PostgreSQLGateway.SQL_TABLE_COMMENT, {"table_name": table_name})
        comments = cursor.fetchall()
        return {comment.relname : comment.description for comment in comments}

    @returns(Table)
    def make_table(self, table_name):
        """
        Build a Table instance according to a given table/view name by
        quering the PostgreSQL schema.

        Args:
            table_name: Name of a view or a relation in PostgreSQL (String instance)
        Returns:
            The Table instance extracted from the PostgreSQL schema related
            to the queried view/relation
        """
        cursor = self.get_cursor()
        table = Table(self.get_platform(), None, table_name, None, None)
        param_execute = {"table_name": table_name}

        # FOREIGN KEYS:
        # We build a foreign_keys dictionary associating each field of
        # the table with the table it references.
        cursor.execute(PostgreSQLGateway.SQL_TABLE_FOREIGN_KEYS, param_execute)
        fks = cursor.fetchall()
        foreign_keys = {fk.column_name: fk.foreign_table_name for fk in fks}

        # COMMENTS:
        # We build a comments dictionary associating each field of the table with
        # its comment.
        comments = self.get_fields_comment(table_name)

        # FIELDS:
        fields = set()
        cursor.execute(PostgreSQLGateway.SQL_TABLE_FIELDS, param_execute)
        for field in cursor.fetchall():
            # PostgreSQL types vs base types
            table.insert_field(Field(
                qualifiers  = [] if field.is_updatable == "YES" else ["const"],
                type        = foreign_keys[field.column_name] if field.column_name in foreign_keys else PostgreSQLGateway.to_manifold_type(field.data_type),
                name        = field.column_name,
                is_array    = (field.data_type == "ARRAY"),
                description = comments[field.column_name] if field.column_name in comments else "(null)"
            ))
    
        # PRIMARY KEYS: XXX simple key ?
        # We build a key dictionary associating each table with its primary key
        cursor.execute(PostgreSQLGateway.SQL_TABLE_KEYS, param_execute)
        fks = cursor.fetchall()

        primary_keys = dict()
        for fk in fks:
            foreign_key = tuple(fk.column_names)
            if table_name not in primary_keys.keys():
                primary_keys[table_name] = set()
            primary_keys[table_name].add(foreign_key)
        
        if table_name in primary_keys.keys():
            for k in primary_keys[table_name]:
                table.insert_key(k)
   
        # PARTITIONS:
        # TODO
    
        #mc = MetadataClass('class', table_name)
        #mc.fields = fields
        #mc.keys.append(primary_keys[table_name])
    
        table.capabilities.retrieve   = True
        table.capabilities.join       = True
        table.capabilities.selection  = True
        table.capabilities.projection = True
        Log.debug("Adding table: %s" % table)
        return table

    @returns(list)
    def make_metadata_from_names(self, table_names):
        """
        Build metadata by querying postgresql's information schema
        Param
            table_names: A structure on which we can iterate
                (list, set, generator...) to retrieve table names
                or view names. Filtered table names are ignored
                (see self.is_ignored_table())
        Returns:
            The list of corresponding Announce instances
        """
        announces_pgsql = list() 
        
        for table_name in table_names:
            if self.is_ignored_table(table_name):
                continue
            announces_pgsql.append(Announce(self.make_table(table_name)))

        announces_pgsql = self.tweak_announces(announces_pgsql)
        return announces_pgsql

    def table_exists(self, table_name):
        ret, = self.selectall(PostgreSQLGateway.SQL_CHECK_TABLE_EXISTS % (table_name,))
        print "table exists", ret
        return ret
