# To avoid naming conflicts when importing 
from __future__ import absolute_import

import csv, os.path

from manifold.gateways                  import Gateway
from manifold.metadata.MetadataClass    import MetadataClass
from manifold.core.announce             import Announce
from manifold.core.capabilities         import Capabilities
from manifold.core.field                import Field 

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

        self.get_metadata()

    def forward(self, query, deferred=False, execute=True, user=None):
        self.query = query
        self.start()

    def start(self):
        assert self.query, "Query should have been associated before start"
        # XXX how to start on multiple files ?
        filename = self.map_filenames[self.query.fact_table]
        with open(filename , 'rb') as csvfile:
            reader = csv.DictReader(csvfile, dialect=self.dialect)
            try:
                for row in reader:
                    self.callback(row)
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
                fields = []
                for field in reader.fieldnames:
                    fields.append(Field(
                        qualifier   = 'const', # unless we want to update the CSV file
                        type        = 'string',
                        name        = field,
                        is_array    = False,
                        description = '(null)'
                    ))

                mc = MetadataClass('class', self.get_base(filename))
                mc.fields = set(fields) # XXX set should be mandatory
                mc.keys = reader.fieldnames[0]

                cap = Capabilities()

                announce = Announce(mc, cap)
                announces.append(announce)

        return announces
        
        # XXX cannot infer the type, should be provided along fields ?
        # Let's assume everything is a string for now...
        print "FIELDNAMES", reader.fieldnames

        

        return []




