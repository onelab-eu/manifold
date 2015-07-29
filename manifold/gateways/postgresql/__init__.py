#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# PostgreSQL Gateway
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Loïc Baron        <loic.baron@lip6.fr>
#
# Copyright (C) 2015 UPMC

import re, datetime, traceback


from types                              import StringTypes, GeneratorType, NoneType, IntType, LongType, FloatType, ListType, TupleType

from manifold.gateways                          import Gateway
from manifold.gateways.postgresql.collection    import PostgreSQLCollection
from manifold.gateways.postgresql.connection import PostgreSQLConnection

from manifold.util.log                          import Log
from manifold.util.misc                         import is_iterable
from manifold.util.type                         import accepts, returns

class PostgreSQLGateway(Gateway):
    # this gateway_name must be used as gateway_type when adding a platform to the local storage 
    __gateway_name__ = "postgresql"

    #-------------------------------------------------------------------------------
    # Metadata 
    #-------------------------------------------------------------------------------

    # Tables

    SQL_DB_TABLE_NAMES = """
    SELECT    table_name 
        FROM  information_schema.tables 
        WHERE table_schema  = 'public'
          AND table_type    = 'BASE TABLE'
          AND table_catalog = '%(db_name)s'
    """

    # Views

    SQL_DB_VIEW_NAMES = """
    SELECT    table_name
        FROM  information_schema.views
        WHERE table_schema  = ANY(current_schemas(false))
          AND table_catalog = '%(db_name)s'
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



