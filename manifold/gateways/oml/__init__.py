#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway managing OML repositories.
# http://mytestbed.net/projects/oml
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

from manifold.core.table                import Table
from manifold.gateways.postgresql       import PostgreSQLGateway
from manifold.core.announce             import Announce
from manifold.core.key                  import Key, Keys
from manifold.core.field                import Field 
from manifold.core.record               import Record
from manifold.util.log                  import Log 
from manifold.util.type                 import accepts, returns 

class OMLGateway(PostgreSQLGateway):
    __gateway_name__ = "oml"

    # The OML gateway provides additional functions compared to PostgreSQL
    def __init__(self, interface, platform, platform_config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to the
                Platform which instantiates this Gateway.
        """
        super(OMLGateway, self).__init__(interface, platform, platform_config)

    @returns(list)
    def get_slice(self, query):
        return [{
            "slice_hrn" : "ple.upmc.myslicedemo",
            "lease_id"  :  100
        }, {
            "slice_hrn" : "ple.upmc.agent",
            "lease_id"  :  101
        }]

# TODO move into oml/methods/application.py
#    def get_application(self, filter=None, params = None, fields = None):
    @returns(list)
    def get_application(self, query):
        fields = query.get_select()
        filter = query.get_where()
        params = query.get_params()

        #print "GET_MEASUREMENT", filter, params, fields
        #print "FORCED LEASE ID TO 100"
        lease_id = 100
        lease_id_str = "%d" % lease_id
        # List databases
        db = self.get_databases()
        if not lease_id_str in db:
            Log.error("Invalid lease ID")

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

        #print "APPLICATION ret=", ret
        return ret

# TODO move into oml/methods/measurement_point.py
    @returns(list)
#    def get_measurement_point(self, filter=None, params = None, fields = None):
    def get_measurement_point(self, query): 
        # Maybe this cannot be called directly ? maybe we rely on get_measurement
        #print "GET_MEASUREMENT_POINT", filter, params, fields

        # Try connection to database 'lease_id', if error return none

        # List measurement points from _experiment_metadata
        return [{'measurement_point': 'counter'}]

# TODO move into oml/methods/measurement_table.py
    @returns(list)
#    def get_measurement_table(measure, filter=None, params=None, fields=None):
    def get_measurement_table(measure, query): 
        # We should be connected to the right database
        #print "OMLGateway::application"#, application
        #print "OMLGateway::measure", measure

        # We need the name of the measure + the application
        application = 'Application1'
        measure = 'counter'
        #print ">> OMLGateway::application", application
        #print ">> OMLGateway::measure", measure

        # Use postgresql query to sql function
        sql = 'SELECT * FROM "%s_%s";' % (application, measure)
        out = self.selectall(sql)
        
    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        self.check_receive(packet)
        query = packet.get_query()

        try:
            # Announced objects: slice, application, measurement_point
            #print "QUERY", query.object, " -- FILTER=", query.filters
            records = getattr(self, "get_%s" % query.get_from())(query)
        except Exception, e:
            # Missing function = we are querying a measure. eg. get_counter
            records = self.get_measurement_table(query.get_from())()

        self.records(packet, records)

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

    @returns(list)
    def make_announces(self):
        """
        Returns:
            The list of corresponding Announce instances
        """
        announces = list() 

        # We will forge metadata manually
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

        t = Table(self.get_platform_name(), "slice")

        slice_hrn = Field(
            qualifiers  = ['const'],
            type        = 'text',
            name        = 'slice_hrn',
            is_array    = False,
            description = 'Slice Human Readable Name'
        )
        t.insert_field(slice_hrn)
        t.insert_field(Field(
            qualifiers  = ['const'],
            type        = 'int',
            name        = 'lease_id',
            is_array    = False,
            description = 'Lease identifier'
        ))
        t.insert_key(slice_hrn)

        t.capabilities.join       = True
        t.capabilities.selection  = True
        t.capabilities.projection = True

        announces.append(Announce(t))

        # ANNOUNCE
        #
        # TABLE application (
        #   lease_id
        #   application
        #  
        # )

        t = Table(self.get_platform_name(), "application")

        lease_id = Field(
            qualifiers  = ['const'],
            type        = 'int',
            name        = 'lease_id',
            is_array    = False,
            description = 'Lease identifier'
        )
        application = Field(
            qualifiers  = ['const'],
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

        t.capabilities.retrieve   = True
        t.capabilities.join       = True
        t.capabilities.selection  = True
        t.capabilities.projection = True

        announces.append(Announce(t))


        # ANNOUNCE
        #
        # TABLE measurement_point (
        #   measurement_point
        #  
        # )

        t = Table(self.get_platform_name(), "measurement_point")

        lease_id = Field(
            qualifiers  = ['const'],
            type        = 'int',
            name        = 'lease_id',
            is_array    = False,
            description = 'Lease identifier'
        )
        application = Field(
            qualifiers  = ['const'],
            type        = 'string',
            name        = 'application',
            is_array    = False,
            description = '(null)'
        )
        measurement_point = Field(
            qualifiers  = ['const'],
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

        t.capabilities.retrieve   = True
        t.capabilities.join       = True
        t.capabilities.selection  = True
        t.capabilities.projection = True

        announces.append(Announce(t))

        return announces
