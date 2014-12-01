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

from manifold.core.announce             import Announce, Announces
from manifold.core.capabilities         import Capabilities
from manifold.core.field                import Field
from manifold.core.key                  import Key
from manifold.core.keys                 import Keys
from manifold.core.record               import Record
from manifold.core.table                import Table
from manifold.gateways                  import Gateway
from manifold.gateways.object           import ManifoldObject, ManifoldCollection
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

class CSVCollection(ManifoldCollection):

    def __init__(self, object_name, config):
        if not isinstance(config, dict):
            raise Exception("Wrong format for field description. Expected dict")

        self._config = config

        if 'fields' in config and 'key' in config:
            dialect, field_names, field_types, has_headers = self.get_dialect_and_field_info()
            self._has_headers = has_headers
            self._dialect     = dialect
            self._field_names = field_names
            self._field_types = field_types
        else:
            # We allow those fields not to be specified since they are not
            # known/needed for writing to a CSV file
            self._has_headers = True
            self._dialect     = None
            self._field_names = None
            self._field_types = None

        self._cls = self.make_object(object_name, config)
        self._csvfile = None
        self._writer = None

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

    def get_dialect_and_field_info(self):
        filename = self._config['filename']

        Log.info("Loading %s" % filename)
        with open(filename, 'rb') as f:
            sample = f.read(1024)
            dialect = csv.Sniffer().sniff(sample)
            has_headers = csv.Sniffer().has_header(sample)

        HAS_FIELDS_OK, HAS_FIELDS_KO, HAS_FIELDS_ERR = range(1,4)
        HAS_TYPES_OK,  HAS_TYPES_KO,  HAS_TYPES_ERR  = range(1,4)

        has_fields = HAS_FIELDS_KO
        has_types  = HAS_TYPES_KO

        if "fields" in self._config:
            try:
                field_names, field_types = [], []
                for name, type in self._config["fields"]:
                    field_names.append(name)
                    has_fields = HAS_FIELDS_OK
                    field_types.append(type)
                    has_types = HAS_TYPES_OK
            except Exception, e:
                Log.warning("Wrong format for fields in platform configuration")
                has_fields = HAS_FIELDS_ERR
                has_types  = HAS_TYPES_ERR

        # If the fields are not specified in config, let's look at CSV headers
        if has_fields in [HAS_FIELDS_KO, HAS_FIELDS_ERR]:
            if not has_headers:
                raise Exception, "Missing field description"
            with open(filename, "rb") as csvfile:
                reader = csv.reader(csvfile, dialect=dialect)
                # Note: we do not use DictReader since we need a list of fields in order
                field_names = reader.next()

        # We try to guess file types if they are not specified
        if has_types in [HAS_TYPES_KO, HAS_TYPES_ERR]:
            with open(filename, "rb") as csvfile:
                reader = csv.reader(csvfile, dialect=dialect)
                if has_headers:
                    values = reader.next()
                values = reader.next() # a list of string
                field_types = list()
                for value in values:
                    field_types.append(self.guess_type(value))

        return (dialect, field_names, field_types, has_headers)

    @returns(list)
    def get_key(self):
        """
        Extract the Fields involved in the key specified of a given
        Table in the platform_config.
        Args:
            table_name: A String containing the name of a Table.
        Returns:
            A list of Strings (the fields involved in the Key).
        """
        # NOTE only a single key is supported
        if not "key" in self._config:
            return None
            #raise RuntimeError("Missing key in platform configuration")

        return self._config["key"].split(",")

    @returns(Capabilities)
    def get_capabilities(self):
        """
        Extract the Capabilities corresponding to a Table.
        Args:
            table_name: A String containing the name of a Table.
        Returns:
            The corresponding Capabilities instance.
        """
        capabilities = Capabilities()

        if "capabilities" in self._config:
            capabilities_str = self._config["capabilities"].split(",")
            for capability_str in capabilities_str:
                setattr(capabilities, capability_str, True)
        else:
            capabilities.retrieve = True
            capabilities.join     = False # Since we cannot filter by key, the join Capabilities must be disabled.

        return capabilities


    def make_object(self, object_name, options):

        fields = dict()
        if self._field_names and self._field_types:
            for name, type in zip(self._field_names, self._field_types):
                field = Field(
                    type        = type,
                    name        = name,
                    qualifiers  = ["const"], # unless we want to update the CSV file
                    is_array    = False,
                    description = "(null)"
                )
                fields[name] = field

        keys = Keys()
        key_field_names = self.get_key()
        if key_field_names:
            if isinstance(key_field_names, StringTypes):
                key_fields = frozenset([fields[key_field_names]])
            elif isinstance(key_field_names, (list, set, frozenset, tuple)):
                key_fields = frozenset([fields[key_elt] for key_elt in key_field_names])
            keys.add(Key(key_fields))

        class obj(ManifoldObject):
            __object_name__ = object_name
            __fields__ = fields.values()
            __keys__   = keys
            __capabilities__ = self.get_capabilities()
        # XXX table.capabilities = self.get_capabilities(table_name)
        
        return obj

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

    def get(self, packet):
        # packet is not used since we do not support filters neither fields
        with open(self._config.get('filename') , "rb") as csvfile:
            reader = csv.reader(csvfile, dialect=self._dialect)
            try:
                if self._has_headers:
                    values = reader.next()

                records = list()
                for row in reader:
                    row = CSVCollection.convert(row, self._field_names, self._field_types)
                    if not row: continue
                    records.append(row)
                self.get_gateway().records(records, packet)

            except csv.Error as e:
                message = "CSVGateway::forward(): Error in file %s, line %d: %s" % (filename, reader.line_num, e)

    def create(self, packet):
        # XXX We might maintain the file open
        if not packet.is_empty():
            data = packet.get_data()
            if not self._writer:
                source = packet.get_source()
                object_name = source.get_object_name()

                # XXX Headers are already stored for csv read
                if source.get_field_names().is_star():
                    self._headers = data.keys()
                else:
                    self._headers = list(source.get_field_names())

                if not self._config:
                    return
                filename = self._config.get('filename')
                if not filename:
                    return
                self._csvfile = open(filename, 'wb')
                self._writer = csv.writer(self._csvfile, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)

                if self._has_headers:
                    self._writer.writerow(self._headers)

            if data:
                row = [data[h] for h in self._headers]
                self._writer.writerow(row)

        if packet.is_last():
            if not self._csvfile:
                print "W: no csvfile"
                return None
            self._csvfile.close()

        return None

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

        for object_name, config in platform_config.items():
            collection = CSVCollection(object_name, config)
            self.register_collection(collection)
