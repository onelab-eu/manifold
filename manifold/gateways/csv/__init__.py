# To avoid naming conflicts when importing 
from __future__ import absolute_import

import csv, os.path
from itertools               import izip
from datetime                import datetime
from manifold.gateways       import Gateway
from manifold.core.record    import Record, LastRecord
from manifold.core.table     import Table
from manifold.core.announce  import Announce
from manifold.core.field     import Field 
from manifold.core.key       import Key
from manifold.types.inet     import inet
from manifold.types.hostname import hostname
from manifold.types          import type_get_name, type_by_name, int, string, inet, date
from manifold.util.log       import Log
# Heuristics for type guessing

heuristics = (
    inet,
    hostname,
    date,
    int,
)


class CSVGateway(Gateway):
    __gateway_name__ = 'csv'

    def __init__(self, router, platform, query, config, user_config, user):
        super(CSVGateway, self).__init__(router, platform, query, config, user_config, user)

        # Mapping:
        # table1:
        #    filename
        #    fields: name, type
        #    key
        # table2:
        #    ...
        self.has_headers = {}

    def convert(self, row, field_names, field_types):
        #return dict([ (name, type_by_name(type)(value)) for value, name, type in izip(row, field_names, field_types)])
        for value, name, type in izip(row, field_names, field_types):
            return Record([ (name, type_by_name(type)(value)) for value, name, type in izip(row, field_names, field_types)])

    def start(self):
        assert self.query, "Query should have been associated before start"

        table = self.query.object

        dialect, field_names, field_types = self.get_dialect_and_field_info(table)
        key = self.get_key(table)

        filename = self.config[table]['filename']

        with open(filename , 'rb') as csvfile:
            reader  = csv.reader(csvfile, dialect=dialect)
            try:
                if self.has_headers[table]:
                    values = reader.next()
                for row in reader:
                    row = self.convert(row, field_names, field_types)
                    if not row: continue
                    self.send(row)
                self.send(LastRecord())
            except csv.Error as e:
                sys.exit('file %s, line %d: %s' % (filename, reader.line_num, e))

    def get_base(self, filename):
        return os.path.splitext(os.path.basename(filename))[0]

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

    def get_metadata(self):


        announces = []

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
