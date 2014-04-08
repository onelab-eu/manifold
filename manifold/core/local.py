#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Manifold router handles Query, compute the corresponding QueryPlan,
# and deduce which Queries must be sent the appropriate Gateways.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

# This is used to manipulate the router internal state
# It should not point to any storage directly unless this is mapped
from manifold.gateways              import Gateway
from manifold.core.announce         import Announce, make_virtual_announces
from manifold.core.dbgraph          import DBGraph
from manifold.core.method           import Method
from manifold.core.record           import Records
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

LOCAL_NAMESPACE      = "local"

class LocalGateway(Gateway):
    """
    Handle queries related to local:object, local:gateway, local:platform, etc.
    """

    def __init__(self, interface = None, platform_name = None, platform_config = None):
        """
        Constructor.
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform_name: A String storing name of the Platform related to this
                Gateway or None.
            platform_config: A dictionnary containing the configuration related
                to the Platform managed by this Gateway. In practice, it should
                correspond to the following value stored in the Storage verifying

                    SELECT config FROM local:platform WHERE platform == "platform_name"
        """
        try:
            from manifold.bin.config import MANIFOLD_STORAGE
            self._storage = MANIFOLD_STORAGE
        except:
            self._storage = None

        super(LocalGateway, self).__init__()

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()

        # Since the original query will be altered, we are making a copy here,
        # so that the pit dictionary is not altered
        new_query = query.clone()

        action = query.get_action()
        table_name = query.get_from()

        if table_name == "object":
            if not action == "get":
                 raise RuntimeError("Invalid action (%s) on '%s::%s' table" % (action, self.get_platform_name(), table_name))
            records = Records([announce.to_dict() for announce in self.get_announces()])
        elif table_name == "gateway":
            # Note that local:column won't be queried since it has no RETRIEVE capability.
            if not action == "get":
                 raise RuntimeError("Invalid action (%s) on '%s::%s' table" % (action, self.get_platform_name(), table_name))
            records = Records([{"type" : gateway_type} for gateway_type in sorted(Gateway.list().keys())])
        elif self._storage:
            records = None
            self._storage.get_gateway().receive_impl(packet)
        else:
            raise RuntimeError("Invalid table '%s::%s'" % (self.get_platform_name(), table_name))

        if records:
            self.records(records, packet)

    @returns(list)
    def get_announces(self):
        """
        Returns:
            The list of corresponding Announces instances.
        """
        announces = list()

        # Objects embedded in the Storage
        if self._storage:
            announces.extend(self._storage.get_gateway().get_announces())

        # Virtual tables ("object", "column", ...)
        virtual_announces = make_virtual_announces(LOCAL_NAMESPACE)
        announces.extend(virtual_announces)

        return announces

    @returns(DBGraph)
    def get_dbgraph(self):
        """
        Returns:
            The DBGraph related to the Manifold Storage.
        """
        # We do not need normalization here, can directly query the Gateway

        # 1) Fetch the Storage's announces and get the corresponding Tables.
        local_announces = self.get_announces()
        local_tables = [announce.get_table() for announce in local_announces]

        # 2) Build the corresponding map of Capabilities
        map_method_capabilities = dict()
        for announce in local_announces:
            table = announce.get_table()
            table_name = table.get_name()
            method = Method(LOCAL_NAMESPACE, table_name)
            capabilities = table.get_capabilities()
            map_method_capabilities[method] = capabilities

        # 3) Build the corresponding DBGraph
        return DBGraph(local_tables, map_method_capabilities)

