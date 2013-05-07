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
            'slice_hrn': 'ple.upmc.myslicedemo',
            'lease_id':  100
        }, {
            'slice_hrn': 'ple.upmc.agent',
            'lease_id':  101
        }]

    def get_application(self, filter=None, params = None, fields = None):
        print "GET_MEASUREMENT", filter, params, fields
        print "FORCED LEASE ID TO 100"
        lease_id = 100
        lease_id_str = "%d" % lease_id
        # List databases
        db = self.get_databases()
        if not lease_id_str in db:
            self.callback(None)

        # Connect to slice database
        self.close()
        self.db_name = lease_id

        # List applications
        out = self.selectall("SELECT value from _experiment_metadata where key != 'start_time';")
        #map_app_mps = {}
        #for app_dict in out:
        #    _, app_mp, fields = app_dict['value'].split(' ', 3)
        #    application, mp = app_mp.split('_', 2)
        #    fields = [field.split(':', 2) for field in fields]
        #    if not application in map_app_mps:
        #        map_app_mps[application] = []
        #    map_app_mps[application].append({'measurement_point': mp})
        #
        #ret = []
        #for app, mps in map_app_mps.items():
        #    ret.append({'lease_id': lease_id, 'application': application, 'measurement_point': mps})

        ret = []
        for app_dict in out:
            _, app_mp, fields = app_dict['value'].split(' ', 3)
            application, mp = app_mp.split('_', 2)
            #fields = [field.split(':', 2) for field in fields]
            ret.append({'lease_id': lease_id, 'application': application})

        print "APPLICATION ret=", ret
        return ret

    def get_measurement_point(self, filter=None, params = None, fields = None):
        # Maybe this cannot be called directly ? maybe we rely on get_measurement
        print "GET_MEASUREMENT_POINT", filter, params, fields

        # Try connection to database 'lease_id', if error return none

        # List measurement points from _experiment_metadata
        return [{'measurement_point': 'counter'}]

    def get_measurement_table(measure, filter=None, params=None, fields=None):
        # We should be connected to the right database
        print "OMLGateway::application"#, application
        print "OMLGateway::measure", measure

        # We need the name of the measure + the application
        application = 'Application1'
        measure = 'counter'
        print ">> OMLGateway::application", application
        print ">> OMLGateway::measure", measure

        # Use postgresql query to sql function
        sql = 'SELECT * FROM "%s_%s";' % (application, measure)
        out = self.selectall(sql)
        

    def start(self):
        try:
            print "QUERY", self.query.fact_table, " -- FILTER=", self.query.filters
            results = getattr(self, "get_%s" % self.query.fact_table)()
        except Exception, e:
            # Missing function = we are querying a measure. eg. get_counter
            results = self.get_measurement_table(self.query.fact_table)()
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
        t.insert_key(slice_hrn)
        cap = Capabilities()
        cap.selection = True
        cap.projection = True

        announce = Announce(t, cap)
        announces.append(announce)

        # ANNOUNCE
        #
        # TABLE application (
        #   lease_id
        #   application
        #  
        # )

        t = Table('oml', None, 'application', None, None)

        lease_id = Field(
            qualifier   = 'const',
            type        = 'integer',
            name        = 'lease_id',
            is_array    = False,
            description = 'Lease identifier'
        )
        application = Field(
            qualifier   = 'const',
            type        = 'string',
            name        = 'application',
            is_array    = True,
            description = '(null)'
        )

        t.insert_field(lease_id)
        t.insert_field(application)

        key = Key([lease_id, application])
        t.insert_key(key)
        #t.insert_key(lease_id)

        cap = Capabilities()
        cap.selection = True
        cap.projection = True

        announce = Announce(t, cap)
        announces.append(announce)

        # ANNOUNCE
        #
        # TABLE measurement_point (
        #   measurement_point
        #  
        # )

        t = Table('oml', None, 'measurement_point', None, None)

        lease_id = Field(
            qualifier   = 'const',
            type        = 'integer',
            name        = 'lease_id',
            is_array    = False,
            description = 'Lease identifier'
        )
        application = Field(
            qualifier   = 'const',
            type        = 'string',
            name        = 'application',
            is_array    = False,
            description = '(null)'
        )
        measurement_point = Field(
            qualifier   = 'const',
            type        = 'string',
            name        = 'measurement_point',
            is_array    = False,
            description = '(null)'
        )
        
        t.insert_field(lease_id)
        t.insert_field(application)
        t.insert_field(measurement_point)

        key = Key([lease_id, application, measurement_point])
        t.insert_key(key)
        #t.insert_key(application)

        cap = Capabilities()
        cap.selection = False
        cap.projection = False

        announce = Announce(t, cap)
        announces.append(announce)

        return announces
