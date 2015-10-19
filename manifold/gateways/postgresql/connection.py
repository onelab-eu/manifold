#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# PostgreSQL Collection
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Loïc Baron        <loic.baron@lip6.fr>
#
# Copyright (C) 2015 UPMC


import psycopg2
import psycopg2.extensions
import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
# UNICODEARRAY not exported yet
psycopg2.extensions.register_type(psycopg2._psycopg.UNICODEARRAY)

from manifold.util.type                 import accepts, returns
from manifold.util.log                  import Log

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
            connection_ok = self.connect_tcp()
        if not connection_ok:
            connection_ok = self.connect_unix()
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


