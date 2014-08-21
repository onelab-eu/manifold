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
import csv, os.path
from itertools                          import izip
from datetime                           import datetime
from types                              import StringTypes

from manifold.core.announce             import Announce
from manifold.core.capabilities         import Capabilities
from manifold.core.field                import Field
from manifold.core.record               import Record
from manifold.core.table                import Table
from manifold.gateways                  import Gateway
from manifold.types.inet                import inet
from manifold.types.hostname            import hostname
from manifold.types                     import type_get_name, type_by_name, int, string, inet, date
from manifold.util.log                  import Log
from manifold.util.type                 import accepts, returns

# Heuristics for type guessing
heuristics = (
    inet,
    hostname,
    date,
    int,
)

class CSVGateway(Gateway):
    __gateway_name__ = "csv"

    def __init__(self, router, platform, platform_config):
        """
        Constructor
        Args:
            router: The Router on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.

                In pratice this dictionnary is built as follows:

                    {
                        "table_name" : {
                            "filename" : "/absolute/path/file/for/this/table.csv",
                            "fields"   : [
                                ["field_name1", "type1"],
                                ...
                            ],
                            "key" : "field_name_i, field_name_j, ..."
                        },
                        ...
                    }
        """
        super(CSVGateway, self).__init__(router, platform, platform_config)
        self.has_headers = dict()

    @staticmethod
    @returns(Record)
    def convert(row, field_names, field_types):
        """
        Translate a row of the CSV file managed by this CSVGateway into
        the corresponding Record instance.
        Args:
            row: A list containing the values of a line of the CSV file
                managed by this CSVGateway.
            field_names: A list of String containing the corresponding
                field names (column names).
            field_names: A list of String containing the corresponding
                field types (column types).
        Returns:
            The corresponding Record instance.
        """
        for value, name, type in izip(row, field_names, field_types):
            return Record([(name, type_by_name(type)(value)) for value, name, type in izip(row, field_names, field_types)])

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()
        #Log.tmp("query = %s" % query)
        table_name = query.get_table_name()
        platform_config = self.get_config()

        dialect, field_names, field_types = self.get_dialect_and_field_info(table_name)
        key = self.get_key(table_name)

        filename = platform_config[table_name]["filename"]

        with open(filename , "rb") as csvfile:
            reader = csv.reader(csvfile, dialect=dialect)
            try:
                if self.has_headers[table_name]:
                    values = reader.next()

                records = list()
                for row in reader:
                    row = CSVGateway.convert(row, field_names, field_types)
                    if not row: continue
                    records.append(row)
                self.records(records, packet)

            except csv.Error as e:
                message = "CSVGateway::forward(): Error in file %s, line %d: %s" % (filename, reader.line_num, e)
                self.warning(query, e)

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
        platform_config = self.get_config()
        t = platform_config[table]
        filename = t["filename"]

        Log.info("Loading %s" % filename)
        with open(filename, 'rb') as f:
            sample = f.read(1024)
            dialect = csv.Sniffer().sniff(sample)
            self.has_headers[table] = csv.Sniffer().has_header(sample)

        HAS_FIELDS_OK, HAS_FIELDS_KO, HAS_FIELDS_ERR = range(1,4)
        HAS_TYPES_OK,  HAS_TYPES_KO,  HAS_TYPES_ERR  = range(1,4)

        has_fields = HAS_FIELDS_KO
        has_types  = HAS_TYPES_KO

        if isinstance(t, dict):
            if "fields" in t:
                try:
                    field_names, field_types = [], []
                    for name, type in t["fields"]:
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
            with open(filename, "rb") as csvfile:
                reader = csv.reader(csvfile, dialect=dialect)
                # Note: we do not use DictReader since we need a list of fields in order
                field_names = reader.next()

        if has_types in [HAS_TYPES_KO, HAS_TYPES_ERR]:
            # We try to guess file types
            with open(t["filename"], "rb") as csvfile:
                reader = csv.reader(csvfile, dialect=dialect)
                if self.has_headers[table]:
                    values = reader.next()
                values = reader.next() # a list of string
                field_types = list()
                for value in values:
                    field_types.append(self.guess_type(value))

        return (dialect, field_names, field_types)

    @returns(StringTypes)
    def guess_type(self, value):
        """
        Args:
            value:
        Returns:
            The corresponding Manifold type.
            See manifold.types
        """
        for type in heuristics:
            try:
                _ = type(value)
                return type_get_name(type)
            except ValueError:
                continue
        # All other heuristics failed it is a string
        return "string"

    @returns(list)
    def get_key(self, table_name):
        """
        Extract the Fields involved in the key specified of a given
        Table in the platform_config.
        Args:
            table_name: A String containing the name of a Table.
        Returns:
            A list of Strings (the fields involved in the Key).
        """
        platform_config = self.get_config()

        # NOTE only a single key is supported
        if not "key" in platform_config[table_name]:
            raise RuntimeError("Missing key in platform configuration")

        return platform_config[table_name]["key"].split(",")

    @returns(Capabilities)
    def get_capabilities(self, table_name):
        """
        Extract the Capabilities corresponding to a Table.
        Args:
            table_name: A String containing the name of a Table.
        Returns:
            The corresponding Capabilities instance.
        """
        capabilities = Capabilities()
        platform_config = self.get_config()

        if "capabilities" in platform_config[table_name]:
            capabilities_str = platform_config[table_name]["capabilities"].split(",")
            for capability_str in capabilities_str:
                setattr(capabilities, capability_str, True)
        else:
            capabilities.retrieve = True
            capabilities.join     = False # Since we cannot filter by key, the join Capabilities must be disabled.

        return capabilities

    @returns(list)
    def make_announces(self):
        """
        Build a list of Announces by loading header files.
        Returns:
            The list of corresponding Announce instances.
        """
        announces = list()
        platform_config = self.get_config()

        for table_name, data in self.get_config().items():
            dialect, field_names, field_types = self.get_dialect_and_field_info(table_name)
            key      = self.get_key(table_name)
            filename = data["filename"]
            table    = Table(self.get_platform_name(), table_name)

            key_fields = set()
            for name, type in zip(field_names, field_types):
                field = Field(
                    type        = type,
                    name        = name,
                    qualifiers  = ["const"], # unless we want to update the CSV file
                    is_array    = False,
                    description = "(null)"
                )
                table.insert_field(field)

                if name in key:
                    key_fields.add(field)

            table.insert_key(key_fields)
            table.capabilities = self.get_capabilities(table_name)
            announces.append(Announce(table))

        return announces
