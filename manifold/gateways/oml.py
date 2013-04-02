from manifold.gateways.postgresql       import PostgreSQLGateway
from manifold.core.table                import Table
from manifold.core.key                  import Key, Keys
from manifold.core.field                import Field 
from manifold.core.announce             import Announce
from manifold.core.capabilities         import Capabilities
import traceback

class OMLGateway(PostgreSQLGateway):

    # The OML gateway provides additional functions compared to Postgresql

    def get_slice(self):
        return [{
            'slice_hrn': 'ple.upmc.myslice_demo',
            'lease_id':  100
        }, {
            'slice_hrn': 'ple.upmc.agent',
            'lease_id':  101
        }]

    def start(self):
        results = getattr(self, "get_%s" % self.query.fact_table)()
        for row in results:
            self.callback(row)
        self.callback(None)
        
        # Hook queries for OML specificities

        # slice_hrn - job_id will be hardcoded for now
        

        # Tables in OML represent = an experiment
        # XXX need to clarify the difference between slices and experiments

        #measurements
        #    database name = job_id = f(slice_hrn) = database name
        #    database table = measurement points =
        #    rows = measurements (common schema)

        # databases with tables whose schemas are based on the measurement
        # points you identified in your original code.


        #super(OMLGateway, self).start()
        #print "DATABASES", self.get_databases()


    # We will forge metadata manually
    def get_metadata(self):


        announces = []

        # ANNOUNCE - HARDCODED 
        #
        # TABLE slice (
        #   slice_hrn
        #   job_id
        #   KEY slice_hrn
        # )
        #
        # - Note the 'const' field specification since all measurements are
        # read only
        # - Here we have an example of a gateway that might not support the
        # same operators on the different tables

        t = Table('oml', None, 'slice', None, None)

        fields = set()
        slice_hrn = Field(
            qualifier   = 'const',
            type        = 'text',
            name        = 'slice_hrn',
            is_array    = False,
            description = 'Slice Human Readable Name'
        )
        t.insert_field(slice_hrn)
        t.insert_field(Field(
            qualifier   = 'const',
            type        = 'integer',
            name        = 'lease_id',
            is_array    = False,
            description = 'Lease identifier'
        ))
        try:
            t.insert_key(slice_hrn)
        except Exception, e:
            print "MC EXC", e
            print traceback.print_exc()
        cap = Capabilities()
        cap.selection = True
        cap.projection = True

        announce = Announce(t, cap)
        announces.append(announce)

#        # ANNOUNCE
#        #
#        # TABLE application (
#        #   lease_id
#        #   application
#        #  
#        # )
#
#        mc = MetadataClass('class', 'application')
#
#        fields = set()
#        fields.add(Field(
#            qualifier   = 'const',
#            type        = 'integer',
#            name        = 'lease_id',
#            is_array    = False,
#            description = 'Lease identifier'
#        ))
#        fields.add(Field(
#            qualifier   = 'const',
#            type        = 'string',
#            name        = 'application_oml',
#            is_array    = False,
#            description = '(null)'
#        ))
#        mc.fields = fields
#        mc.keys.append('lease_id')
#        #mc.partitions.append()
#
#        cap = Capabilities()
#        cap.selection = True
#        cap.projection = True
#
#        announce = Announce(mc, cap)
#        announces.append(announce)
#
#        # ANNOUNCE
#        #
#        # TABLE measurement_point (
#        #   measurement_point
#        #  
#        # )
#
#        mc = MetadataClass('class', 'measurement_point')
#
#        fields = set()
#        fields.add(Field(
#            qualifier   = 'const',
#            type        = 'integer',
#            name        = 'lease_id',
#            is_array    = False,
#            description = 'Lease identifier'
#        ))
#        fields.add(Field(
#            qualifier   = 'const',
#            type        = 'string',
#            name        = 'application_oml',
#            is_array    = False,
#            description = '(null)'
#        ))
#        mc.fields = fields
#        mc.keys.append('lease_id')
#        #mc.partitions.append()
#
#        cap = Capabilities()
#        cap.selection = True
#        cap.projection = True
#
#        announce = Announce(mc, cap)
#        announces.append(announce)



        
        return announces
