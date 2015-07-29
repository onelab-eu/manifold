#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Example of a Gateway
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Loïc Baron        <loic.baron@lip6.fr>
#
# Copyright (C) 2015 UPMC

import re, datetime, traceback

import time # for timing sql calls

from types                              import StringTypes, GeneratorType, NoneType, IntType, LongType, FloatType, ListType, TupleType

import psycopg2
import psycopg2.extensions
import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
# UNICODEARRAY not exported yet
psycopg2.extensions.register_type(psycopg2._psycopg.UNICODEARRAY)

from manifold.core.capabilities         import Capabilities
from manifold.core.object               import ObjectFactory
from manifold.core.field                import Field
from manifold.core.key                  import Key
from manifold.core.keys                 import Keys
from manifold.core.query                import Query

from manifold.gateways                  import Gateway
from manifold.gateways.object           import ManifoldCollection

from manifold.util.log                  import Log
from manifold.util.misc                 import is_iterable
from manifold.util.type                 import accepts, returns

class PostgreSQLGateway(Gateway):
    # this gateway_name must be used as gateway_type when adding a platform to the local storage 
    __gateway_name__ = "postgresql"

    #-------------------------------------------------------------------------------
    # Metadata 
    #-------------------------------------------------------------------------------

    SQL_STR = """
    SELECT %(fields)s
        FROM %(table_name)s
        %(where)s
    """;

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
                 array_agg(kcu.column_name::text)   AS column_names
        FROM     information_schema.table_constraints AS tc   
            JOIN information_schema.key_column_usage  AS kcu ON tc.constraint_name = kcu.constraint_name
        WHERE    constraint_type = 'PRIMARY KEY' AND tc.table_name = %(table_name)s
        GROUP BY tc.table_name;
    """

    # in case of failure of SQL_TABLE_KEYS execute SQL_TABLE_KEYS_2
    SQL_TABLE_KEYS_2 = """
    SELECT      table_name AS table_name, array_agg(column_name::text) AS column_names 
        FROM information_schema.key_column_usage 
        WHERE table_name = %(table_name)s 
        AND constraint_name LIKE '%%_pkey'
        GROUP BY table_name;
    """

    SQL_TABLE_FOREIGN_KEYS = """
    SELECT replace(replace(cons.conname, %(table_name)s || '_', ''), '_fkey', '') AS column_name, c2.relname AS foreign_table_name
        FROM pg_class c                                          
            JOIN            pg_namespace n ON n.oid = c.relnamespace
            LEFT OUTER JOIN pg_constraint cons ON cons.conrelid = c.oid
            LEFT OUTER JOIN pg_class c2 ON cons.confrelid = c2.oid
            LEFT OUTER JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
        WHERE c.relkind = 'r' 
            AND n.nspname IN ('public') -- any other schemas in here
            AND (cons.contype = 'f' OR cons.contype IS NULL)
            AND c.relname = %(table_name)s;
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
    NONE_TABLE = list() 


    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    # XXX Don't we use .h files anymore?

    def __init__(self, router, platform, **platform_config):
        """
        Constructor of a PostgreSQLGateway instance
        Args:
            router: The Router on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.

            re_ignored_tables: A list of re instances filtering tables that must be
                not processed by PostgreSQLGateway. For instance you could filter tables
                not exposed to Manifold. You may also pass:
                - ANY_TABLE:  every table are ignored if not explicitly accepted
                - NONE_TABLE: no table is filtered
            re_allowed_tables: A list of re instances allowing tables. This supersedes
                table filtered by re_ignored_tables regular expressions. You may
                also pass

            table_aliases: A { String : String } dictionnary maps each Manifold object
                name with its corresponding pgsql table/view name.
                If the both names match, you do not need to provide alias.
                
                Example:
                
                  self.table_aliases = {
                      "my_object_name" : "my_table_name",
                      "foo"            : "view_foo"
                  }

            custom_keys: A {String : list(list(String))} dictionnary is used to inject
                additional Keys (list of field names) in the Manifold object not
                declared in the pgsql schema. These custom keys may involve custom
                fields.
                
                Example:
                  custom_keys = {
                      "agent" : [["ip", "platform"]]
                  }

            custom_fields: A {String : list(Field)} dictionnary is used to inject
                additional Fields in the Manifold object which correspond
                to columns not declared in the pgsql schema. The Gateway
                is supposed to inject the appropriate value in the returned
                records.
                
                Example:
                  custom_fields = {
                      "agent" : [
                          Field("const", "string", "my_field_name", None, "My description")
                      ]
                  }

        """
        super(PostgreSQLGateway, self).__init__(router, platform, **platform_config)

        self.cnx    = PostgreSQLConnection(platform_config)
        self.cursor = self.cnx.get_cursor()
        
        # The table matching those regular expressions are ignored...
        # ... excepted the ones explicitly allowed
        self.re_ignored_tables = platform_config.get('re_ignored_tables', self.NONE_TABLE)
        self.re_allowed_tables = platform_config.get('re_allowed_tables', self.ANY_TABLE)
        self.table_aliases     = platform_config.get('table_aliases', dict())
        self.custom_fields     = platform_config.get('custom_fields', dict())
        self.custom_keys       = platform_config.get('custom_keys', dict())


        objects = self.get_objects(platform_config)
        for object_name, config in objects:
            collection = PostgreSQLCollection(object_name, config, platform_config)
            self.register_collection(collection)

    @returns(list)
    def get_objects(self, platform_config):
        """
        Get the list of objects advertised by the platform 
        Args:
            platform_config

        Static data model
            returns the list of objects stored in local platform_config
            [(object_name,{config}),(object_name,{config})]

        Dynamic data model
            get the list of objects from the platform

        Returns:
            A list of objects.
        """

        objects = list()

        # Dynamic data model
        # -----------------------------
        # Format should be as follows: [(object_name,{config}),(object_name,{config})]
        table_names = self.get_table_names()
        for table_name in table_names:
            if self.is_ignored_table(table_name): continue
            #table = self.make_table(table_name)
            #table = self.tweak_table(table)
            objects.append((table_name,{}))
        return objects

    @returns(GeneratorType)
    def get_table_names(self):
        """
        Retrieve the table names stored in the current database
        Returns:
            A generator allowing to iterate on each table names (String instance)
        """
        return self._get_generator(PostgreSQLGateway.SQL_DB_TABLE_NAMES % self.get_config())

    @returns(GeneratorType)
    def _get_generator(self, sql_query):
        """
        (Internal usage)
        Build a generator allowing to iterate on the first
        field of a set of queried records
        Args:
            sql_query: A SQL query passed to PostgreSQL (String instance)
        Returns:
            The corresponding generator instance
        """
        cursor = self.cursor
        cursor.execute(sql_query)
        return (record[0] for record in cursor.fetchall())

    # TODO this could be moved into Gateway to implement "Access List"
    # TODO see manifold/policy
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
    
    #---------------------------------------------------------------------------
    # Announces (TODO move this in Gateway and/or Announce) 
    #---------------------------------------------------------------------------

#    @returns(Table)
#    def tweak_table(self, table):
#        """
#        Update a Table instance according to tweaks described in
#        self.custom_fields and self.custom_keys
#        Args:
#            table: A reference to this Table.
#        Returns:
#            The updated Table.
#        """
#        table_name = table.get_name()
#
#        # Inject custom fields in their corresponding announce
#        if table_name in self.custom_fields.keys():
#            for field in self.custom_fields[table_name]:
#                table.insert_field(field)
#
#        # Inject custom keys in their corresponding announce
#        if table_name in self.custom_keys.keys():
#            for key in self.custom_keys[table_name]:
#                table.insert_key(key)
#
#        return table


#---------------------------------------------------------------------------
# Connection 
#---------------------------------------------------------------------------
class PostgreSQLConnection():

    def __init__(self, config):
        self._config = config
        self.cursor      = None
        self.connection  = None

        self.description = None
        self.lastrowid   = None
        self.rowcount    = None

    def get_description(self):
        return self.description
    def get_lastrowid(self):
        return self.lastrowid
    def get_rowcount(self):
        return self.rowcount

    def get_cursor(self, cursor_factory = None):
        """
        Retrieve the cursor used to interact with the PostgreSQL server.
        Args:
            cursor_factory: see http://initd.org/psycopg/docs/extras.html
        Returns:
            The cursor used to interact with the PostgreSQL server.
        """
        return self.cursor if self.cursor else self.connect(cursor_factory = psycopg2.extras.NamedTupleCursor)

    @returns(dict)
    def make_psycopg2_config(self):
        """
        Prepare the dictionnary needed to prepare a PostgreSQL connection by
        using psycopg2 based on the self.get_config() result. 
        Returns:
            The corresponding psycopg2-compliant dictionnary
        """
        config = self._config
        return {
            "user"     : config["db_user"],
            "password" : config["db_password"],
            "database" : config["db_name"] if "db_name" in config else self.DEFAULT_DB_NAME,
            "host"     : config["db_host"],
            "port"     : config["db_port"] if "db_port" in config else self.DEFAULT_PORT 
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
        #self.rowcount    = None
        #self.description = None
        #self.lastrowid   = None

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

class PostgreSQLCollection(ManifoldCollection):

    def __init__(self, object_name, config, platform_config):
        if not isinstance(config, dict):
            raise Exception("Wrong format for field description. Expected dict")

        self._config = config
        self.cnx    = PostgreSQLConnection(platform_config)
        self.cursor = self.cnx.get_cursor()

        # Static data model
        # Get fields and key from config of the platform 
        if 'fields' in config and 'key' in config:
            field_names, field_types = self.get_fields_from_config()
            self._field_names = field_names
            self._field_types = field_types
        # Dynamic data model
        # fields not specified must be discovered by the GW
        else:
            self._field_names = None
            self._field_types = None

        self._cls = self.make_object(object_name, config)

    def get_fields_from_config(self):
        try:
            field_names, field_types = None, None
            for name, type in self._config["fields"]:
                field_names.append(name)
                field_types.append(type)
        except Exception, e:
            Log.warning("Wrong format for fields in platform configuration")

        return (field_names, field_types)

    def make_object(self, object_name, options):
        """
        Build an Object instance according to a given table/view name by
        querying the PostgreSQL schema.

        Args:
            object_name: Name of a view or a relation in PostgreSQL (String instance)
        Returns:
            The Object instance extracted from the PostgreSQL schema
        """
        fields = dict()

        cursor = self.cursor
        param_execute = {"table_name": object_name}

        # FOREIGN KEYS:
        # We build a foreign_keys dictionary associating each field of
        # the table with the table it references.
        start_time = time.time()
        cursor.execute(PostgreSQLGateway.SQL_TABLE_FOREIGN_KEYS, param_execute)
        fks = cursor.fetchall()
        foreign_keys = {fk.column_name: fk.foreign_table_name for fk in fks}

        # COMMENTS:
        # We build a comments dictionary associating each field of the table with
        # its comment.
        start_time = time.time()
        comments = self.get_fields_comment(object_name)

        # FIELDS:
        start_time = time.time()
        cursor.execute(PostgreSQLGateway.SQL_TABLE_FIELDS, param_execute)
        for field in cursor.fetchall():
            _qualifiers = list()
            if not field.is_updatable == "YES":
                _qualifiers.append('const')
            is_local = lambda field_name: field_name.endswith('_id')
            if is_local(field.column_name):
                _qualifiers.append('local')

            field = Field(
                type        = foreign_keys[field.column_name] if field.column_name in foreign_keys else self.to_manifold_type(field.data_type),
                name        = field.column_name,
                qualifiers  = _qualifiers,
                is_array    = (field.data_type == "ARRAY"),
                description = comments[field.column_name] if field.column_name in comments else "(null)"
            )
            fields[field.name] = field

        # PRIMARY KEYS: XXX simple key ?
        # We build a key dictionary associating each table with its primary key
        start_time = time.time()
        cursor.execute(PostgreSQLGateway.SQL_TABLE_KEYS, param_execute)
        pks = cursor.fetchall()
        #print "SQL took", time.time() - start_time, "s", "[get_pk]"
        if len(pks) == 0:
            #param_execute['constraint_name'] = '_pkey'
            cursor.execute(PostgreSQLGateway.SQL_TABLE_KEYS_2, param_execute)
            pks = cursor.fetchall()

        keys = Keys()
        l_fields = list()
        for pk in pks:
            primary_key = tuple(pk.column_names)
            for k in primary_key:
                l_fields.append(fields[k])
            keys.add(Key(l_fields))

        obj = ObjectFactory(object_name)

        obj.set_fields(fields.values())
        obj.set_keys(keys)
        obj.set_capabilities(self.get_capabilities())
        
        return obj

    @returns(Capabilities)
    def get_capabilities(self):
        """
        Extract the Capabilities from platform.config
        Returns:
            The corresponding Capabilities instance.
        """
        capabilities = Capabilities()

        # Default capabilities if they are not retrieved
        capabilities.retrieve   = True
        capabilities.join       = True
        capabilities.selection  = True
        capabilities.projection = True
           
        return capabilities

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
        cursor = self.cursor
        cursor.execute(PostgreSQLGateway.SQL_TABLE_COMMENT, {"table_name": table_name})
        comments = cursor.fetchall()
        return {comment.relname : comment.description for comment in comments}

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
            x = "(%s)" % ",".join(map(lambda x: str(PostgreSQLCollection._to_sql_value(x)), x))
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
            return "ARRAY[%s]" % ", ".join(map(PostgreSQLCollection.quote, value))
        else:
            return PostgreSQLCollection._to_sql_value(value)

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
                        value_elt = map(PostgreSQLCollection._to_sql_value, value_elt)
                        predicate_list = ["%s = %s" % (f, ve) for f, ve in izip(field,value_elt)]
                        and_clauses.append(" AND ".join(predicate_list))
                    field = ""
                    op    = ""
                    value = " OR ".join(and_clauses)
                else:
                    value = map(PostgreSQLCollection.quote, value)
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
                value = str(PostgreSQLCollection.quote(value))
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
                    value = str(PostgreSQLCollection.quote(value))
                elif isinstance(value, datetime.datetime):
                    value = str(PostgreSQLCollection.quote(str(value)))

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
        return " AND ".join([PostgreSQLCollection._to_sql_where_elt(predicate) for predicate in predicates])


    @staticmethod
    def to_sql(query):
        """
        Translate self.query in the corresponding postgresql command
        Args:
            query: A Query instance
        Returns:
            A String containing a postgresql command 
        """
        table_name = query.get_table_name()
        if not table_name: Log.error("PostgreSQLCollection::to_sql(): Invalid query: %s" % query)

        select = query.get_select()
        where  = PostgreSQLCollection.to_sql_where(query.get_where())
        params = {
            "fields"     : "*" if select.is_star() else ", ".join(select),
            "table_name" : table_name,
            "where"      : "WHERE %s" % where if where else ""
        }

        sql = PostgreSQLGateway.SQL_STR % params
        Log.tmp(sql)
        return sql

    # see instead: psycopg2.extras.NamedTupleCursor
    @returns(list)
    def selectall(self, query, params = None, hashref = True, key_field = None):
        """
        Return each row as a dictionary keyed on field name (like DBI
        selectrow_hashref()). If key_field is specified, return rows
        as a dictionary keyed on the specified field (like DBI
        selectall_hashref()).

        If params is specified, the specified parameters will be bound
        to the query.

        Args:
            sql: a String containing a SQL query.
            params:
            hashref:
            key_field:
        Returns:
            Returns a list of dict corresponding to fetched records.
        """
        start_time = time.time()
        self.cursor = self.cnx.get_cursor()
        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()
        if hashref or key_field is not None:
            # Return each row as a dictionary keyed on field name
            # (like DBI selectrow_hashref()).
            labels = [column[0] for column in self.cursor.description]
            rows = [dict(zip(labels, row)) for row in rows]

        #print "SQL took", time.time() - start_time, "s", "[", query, "]"
        self.cursor.close()
        self.cnx.commit()

        if key_field is not None and key_field in labels:
            # Return rows as a dictionary keyed on the specified field
            # (like DBI selectall_hashref()).
            return dict([(row[key_field], row) for row in rows])
        else:
            return rows

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
        elif sql_type in ["real","double precision"]:
            return "double"
        elif sql_type == "inet":
            return "ip" # XXX shoud such a matching be automatically done ?
        elif sql_type in ["cidr", "text", "interval"]:
            return sql_type
        elif re_timestamp.match(sql_type):
            return "timestamp"
        else:
            Log.warning("PostgreSQLCollection to_manifold_type: %r might not be supported" % sql_type)
            return sql_type


    def get(self, packet):
        #  this example will just send an empty list of Records
        try:
            records = list()

            # -----------------------------
            #      Add your code here 
            # -----------------------------
            # Get data records from your platform

            # packet is used if the GW supports filters and fields selection
            Log.tmp(packet)
            query = Query.from_packet(packet)
            Log.tmp(query)
            sql = self.to_sql(query)
            records = self.selectall(sql, None)
            # send the records
            self.get_gateway().records(records, packet)

        except Exception as e:
            traceback.print_exc()
            raise Exception("Error in PostgreSQLCollection on get() function: %s" % e)

    def create(self, packet):

        if not packet.is_empty():

            data = packet.get_data()
            source = packet.get_source()
            object_name = source.get_object_name()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Create a record in your platform

        if packet.is_last():
            Log.info("Last record")

    def update(self, packet):

        if not packet.is_empty():

            data = packet.get_data()
            source = packet.get_source()
            object_name = source.get_object_name()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Update a record in your platform

        if packet.is_last():
            Log.info("Last record")

    def delete(self, packet):

        if not packet.is_empty():

            data = packet.get_data()
            source = packet.get_source()
            object_name = source.get_object_name()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Delete a record in your platform

        if packet.is_last():
            Log.info("Last record")

    def execute(self, packet):

        if not packet.is_empty():

            data = packet.get_data()
            source = packet.get_source()
            object_name = source.get_object_name()

            # -----------------------------
            #      Add your code here 
            # -----------------------------

            # Execute a method on your platform

        if packet.is_last():
            Log.info("Last record")
