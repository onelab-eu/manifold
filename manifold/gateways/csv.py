# To avoid naming conflicts when importing 
from __future__ import absolute_import

import csv, os.path
from datetime               import datetime
from manifold.gateways      import Gateway
from manifold.core.table    import Table
from manifold.core.announce import Announce
from manifold.core.field    import Field 
from manifold.core.key      import Key

# Heuristics for type guessing

heuristics = (
    lambda value: datetime.strptime(value, "%Y-%m-%d"),
    int,
    float,
)


class CSVGateway(Gateway):

    def __init__(self, router, platform, query, config, user_config, user):
        super(CSVGateway, self).__init__(router, platform, query, config, user_config, user)
        self.map_filenames = {}

        if not isinstance(self.config['filename'], list):
            self.config['filename'] = [self.config['filename']]
        
        for filename in self.config['filename']:

            table = self.get_base(filename)
            self.map_filenames[table] = filename

            with open(filename, 'rb') as csvfile:
                sample = csvfile.read(1024)
                self.dialect = csv.Sniffer().sniff(sample)
                self.has_headers = csv.Sniffer().has_header(sample)

            # XXX FIELDS PER FILE
            assert self.has_headers or 'fields' in self.config, "Missing fields for csv file"
            self.field_source = self.config['fields'].split(',') if 'fields' in self.config else reader.fieldnames

        self.get_metadata()

    def convert(self, value):
        for type in heuristics:
            try:
                return type(value)
            except ValueError:
                continue
        # All other heuristics failed it is a string
        return value

    def convert_dict(self, dic):
        if self.has_headers:
            return dict([ (k, convert(v)) for k, v in dic.items()])
        else:
            # keys are wrong, we replace them
            return dict(zip(self.field_source, dic.values()))
        #for k, v in dic.items():
        #    dic[k] = convert(v)
        return dic

    def start(self):
        assert self.query, "Query should have been associated before start"
        # XXX how to start on multiple files ?
        filename = self.map_filenames[self.query.object]
        with open(filename , 'rb') as csvfile:
            reader = csv.DictReader(csvfile, dialect=self.dialect)
            try:
                for row in reader:
                    self.callback(self.convert_dict(row))
                self.callback(None)
            except csv.Error as e:
                sys.exit('file %s, line %d: %s' % (filename, reader.line_num, e))

        return 

    def get_base(self, filename):
        return os.path.splitext(os.path.basename(filename))[0]

    def get_metadata(self):


        announces = []

        for filename in self.config['filename']:
            with open(filename, 'rb') as csvfile:
                reader = csv.DictReader(csvfile, dialect=self.dialect)
                key =  self.config['key'].split(',') if 'key' in self.config else field_source[0]

                t = Table(self.platform, None, self.get_base(filename), None, None)

                key_fields = set()
                for field in self.field_source:
                    f = Field(
                        qualifier   = 'const', # unless we want to update the CSV file
                        type        = 'string',
                        name        = field,
                        is_array    = False,
                        description = '(null)'
                    )
                    t.insert_field(f)
                
                    if field in key:
                        key_fields.add(f)

                t.insert_key(key_fields)

                t.capabilities.retrieve   = True
                t.capabilities.join       = True

                announces.append(Announce(t))

        return announces
