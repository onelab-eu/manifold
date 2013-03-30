from manifold.gateways.postgresql       import PostgreSQLGateway
from manifold.metadata.MetadataClass    import MetadataClass
from manifold.core.field                import Field 
from manifold.core.announce             import Announce
from manifold.core.capabilities         import Capabilities

class OMLGateway(PostgreSQLGateway):

    

    # The OML gateway provides additional functions compared to Postgresql

    def start(self):
        
        # Hook queries for OML specificities

        # slice_hrn - job_id will be hardcoded for now
        

        # Tables in OML represent = an experiment
        # XXX need to clarify the difference between slices and experiments

        measurements
            database name = job_id = f(slice_hrn) = database name
            database table = measurement points =
            rows = measurements (common schema)

        # databases with tables whose schemas are based on the measurement
        # points you identified in your original code.


        super(OMLGateway, self).start()


    # We will forge metadata manually
    def get_metadata(self):
        announces = []

        fields = set()
        fields.add(Field(
            qualifier   = 'const',      # measurements are read only
            type        = 'integer',    # ?
            name        = 'DUMMY',      # ?
            is_array    = False,
            description = '(null)'
        ))

        mc = MetadataClass('class', 'measurements')
        mc.fields = fields
        mc.keys.append('xxx')
        #mc.partitions.append()

        # Here we have an example of a gateway that might not support the same
        # operators on the different tables
        cap = Capabilities()
        cap.selection = True
        cap.projection = True

        announce = Announce(mc, cap)
        announces.append(announce)

        return announces
