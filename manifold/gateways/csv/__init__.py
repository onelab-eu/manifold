#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway used to manage platforms storing their information using CSV file.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 


# To avoid naming conflicts when importing 
from __future__                         import absolute_import

import csv, os.path
from types                              import StringTypes
from itertools                          import izip
from datetime                           import datetime

from manifold.core.announce             import Announce
from manifold.core.field                import Field 
from manifold.core.key                  import Key
from manifold.core.table                import Table
from manifold.gateways                  import Gateway
from manifold.operators                 import LAST_RECORD
from manifold.types.inet                import inet
from manifold.types.hostname            import hostname
from manifold.types                     import type_get_name, type_by_name, int, string, inet, date
from manifold.util.log                  import Log
from manifold.util.type                 import returns, accepts
# Heuristics for type guessing

heuristics = (
    inet,
    hostname,
    date,
    int,
)

class CSVGateway(Gateway):

    def __init__(self, router, platform, config):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            config: A dictionnary containing the configuration related to this Gateway.
                It may contains the following keys:
                "name" : name of the platform's maintainer. 
                "mail" : email address of the maintainer.
        """

        super(CSVGateway, self).__init__(router, platform, config)

        # Mapping:
        # table1:
        #    filename
        #    fields: name, type
        #    key
        # table2:
        #    ...
        self.has_headers = {}

    @returns(dict)
    def convert(self, row, field_names, field_types):
        #return dict([ (name, type_by_name(type)(value)) for value, name, type in izip(row, field_names, field_types)])
        for value, name, type in izip(row, field_names, field_types):
            return dict([ (name, type_by_name(type)(value)) for value, name, type in izip(row, field_names, field_types)])

    def forward(self, query, callback, is_deferred = False, execute = True, user = None, format = "dict", from_node = None):
        """
        Query handler.
        Args:
            query: A Query instance, reaching this Gateway.
            callback: The function called to send this record. This callback is provided
                most of time by a From Node.
                Prototype : def callback(record)
            is_deferred: A boolean.
            execute: A boolean set to True if the treatement requested in query
                must be run or simply ignored.
            user: The User issuing the Query.
            format: A String specifying in which format the Records must be returned.
            from_node : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        assert isinstance(query, Query), "Invalid query"

        identifier = from_node.get_identifier() if from_node else None
        table_name = query.get_from()

        dialect, field_names, field_types = self.get_dialect_and_field_info(table_name)
        key = self.get_key(table_name)

        filename = self.config[table_name]['filename']

        with open(filename , 'rb') as csvfile:
            reader = csv.reader(csvfile, dialect=dialect)
            try:
                if self.has_headers[table_name]:
                    values = reader.next()
                for row in reader:
                    row = self.convert(row, field_names, field_types)
                    if not row: continue
                    self.send(row, callback, identifier)
                self.send(LAST_RECORD, callback, identifier)
                self.success(from_node, query)

            except csv.Error as e:
                message = "CSVGateway::forward(): Error in file %s, line %d: %s" % (filename, reader.line_num, e)
                Log.warning(message)
                self.error(from_node, query, message)


#UNUSED|    @staticmethod
#UNUSED|    @returns(StringTypes)
#UNUSED|    def get_base(self, filename):
#UNUSED|        """
#UNUSED|        Extract the name of the base according to its filename.
#UNUSED|        Args:
#UNUSED|            filename: A String containing the path related to the CSV file.
#UNUSED|        Returns:
#UNUSED|            The corresponding base name.
#UNUSED|            Example : CSVGateway.get_base("/foo/bar.csv") returns "bar". 
#UNUSED|        """
#UNUSED|        return os.path.splitext(os.path.basename(filename))[0]

    def get_dialect_and_field_info(self, table):
        t = self.config[table]
        filename = t['filename']

        with open(filename, 'rb') as f:
            sample = f.read(1024)
            dialect = csv.Sniffer().sniff(sample)
            self.has_headers[table] = csv.Sniffer().has_header(sample)

        HAS_FIELDS_OK, HAS_FIELDS_KO, HAS_FIELDS_ERR = range(1,4)
        HAS_TYPES_OK,  HAS_TYPES_KO,  HAS_TYPES_ERR  = range(1,4)
        
        has_fields = HAS_FIELDS_KO
        has_types  = HAS_TYPES_KO

        if isinstance(t, dict):
            if 'fields' in t:
                try:
                    field_names, field_types = [], []
                    for name, type in t['fields']:
                        field_names.append(name)
                        has_fields = HAS_FIELDS_OK
                        field_types.append(type)
                        has_types = HAS_TYPES_OK
                except Exception, e:
                    Log.warning("Wrong format for fields in platform configuration")
                    has_fields = HAS_FIELDS_ERR
                    has_types  = HAS_TYPES_ERR
        else:
            Log.warning("Wrong format for field description. Expected dict")

        if has_fields in [HAS_FIELDS_KO, HAS_FIELDS_ERR]:
            if not self.has_headers[table]:
                raise Exception, "Missing field description"
            with open(filename, 'rb') as csvfile:
                reader = csv.reader(csvfile, dialect=dialect)
                # Note: we do not use DictReader since we need a list of fields in order
                field_names = reader.next()

        if has_types in [HAS_TYPES_KO, HAS_TYPES_ERR]:
            # We try to guess file types
            with open(t['filename'], 'rb') as csvfile:
                reader = csv.reader(csvfile, dialect=dialect)
                if self.has_headers[table]:
                    values = reader.next()
                values = reader.next() # a list of string
                field_types = []
                for value in values:
                    field_types.append(self.guess_type(value))
                    
            
        return (dialect, field_names, field_types)

    def guess_type(self, value):
        for type in heuristics:
            try:
                _ = type(value)
                return type_get_name(type)
            except ValueError:
                continue
        # All other heuristics failed it is a string
        return 'string'
        
    def get_key(self, table):
        # NOTE only a single key is supported
        if not 'key' in self.config[table]:
            raise Exception, "Missing key in platform configuration"
        return self.config[table]['key'].split(',')

    @returns(list)
    def get_metadata(self):
        """
        Build metadata by loading header files
        Returns:
            The list of corresponding Announce instances
        """
        announces = list() 

        for table, data in self.config.items():

            dialect, field_names, field_types = self.get_dialect_and_field_info(table)
            key = self.get_key(table)

            filename = data['filename']

            t = Table(self.platform, None, table, None, None)

            key_fields = set()
            for name, type in zip(field_names, field_types):
                f = Field(
                    qualifiers  = ['const'], # unless we want to update the CSV file
                    type        = type,
                    name        = name,
                    is_array    = False,
                    description = '(null)'
                )
                t.insert_field(f)
                
                if name in key:
                    key_fields.add(f)

            t.insert_key(key_fields)

            t.capabilities.retrieve   = True
            t.capabilities.join       = True

            announces.append(Announce(t))

        return announces
