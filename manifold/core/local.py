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

import traceback
from types                          import StringTypes

from manifold.gateways              import Gateway
from manifold.core.announce         import Announces, announces_from_docstring
from manifold.core.dbgraph          import DBGraph
from manifold.core.method           import Method
from manifold.core.record           import Records
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

LOCAL_NAMESPACE = "local"

@returns(Announces)
def make_local_announces(platform_name):
    """
    Craft an Announces instance storing the the local Tables that
    will be stored in the local DbGraph of a router.
    Args:
        platform_name: A name of the Storage platform.
    Returns:
        The corresponding list of Announces.
    """
    assert isinstance(platform_name, StringTypes)

    @announces_from_docstring(platform_name)
    def make_local_announces_impl():
        """
        class object {
            string  table;           /**< The name of the object/table.        */
            column  columns[];       /**< The corresponding fields/columns.    */
            string  capabilities[];  /**< The supported capabilities           */
            string  key[];           /**< The keys related to this object      */
            string  origins[];       /**< The platform originating this object */

            CAPABILITY(retrieve);
            KEY(table);
        };

        class column {
            string qualifier;
            string name;
            string type;
            string description;
            bool   is_array;

            LOCAL KEY(name);
        };

        class gateway {
            string type;

            CAPABILITY(retrieve);
            KEY(type);
        };
        """

    announces = make_local_announces_impl(platform_name)
    return announces


class LocalGateway(Gateway):
    """
    Handle queries related to local:object, local:gateway, local:platform, etc.
    """

    def __init__(self, router = None, platform_name = None, platform_config = None):
        """
        Constructor.
        Args:
            router: The Router on which this LocalGateway is running.
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
        except Exception, e:
            Log.warning(traceback.format_exc())
            Log.warning("LocalGateway: cannot load Storage.")
            self._storage = None

        super(LocalGateway, self).__init__(router, platform_name, platform_config)

    def receive(self, packet):
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
        table_name = query.get_table_name()

        if table_name == "object":
            if not action == "get":
                 raise RuntimeError("Invalid action (%s) on '%s::%s' table" % (action, self.get_platform_name(), table_name))
            records = Records([table.to_dict() for table in self._router.get_dbgraph().get_announce_tables()])
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

    @returns(Announces)
    def get_announces(self):
        """
        Returns:
            The list of corresponding Announces instances.
        """
        local_announces = Announces()

        # Fetch Announces produced by the Storage
        gateway_storage = self._storage.get_gateway()
        if gateway_storage:
            #local_announces = local_announces | gateway_storage.get_announces()
            local_announces |= gateway_storage.get_announces()

        # Fetch Announces produced by each enabled platform.
        router = self.get_router()
        if router:
            for platform_name in router.get_enabled_platform_names():
                gateway = router.get_gateway(platform_name)
                # foo:object is renamed local:object since we cannot compute query plan
                # over the local DBGraph if its table are not attached to platform "local"
                local_announces |= make_local_announces(LOCAL_NAMESPACE)
        else:
            Log.warning("The router of this %s is unset. Some Announces cannot be fetched" % self)

        return local_announces

    @returns(DBGraph)
    def make_dbgraph(self):
        """
        Make the DBGraph.
        Returns:
            The DBGraph related to the Manifold Storage.
        """
        # We do not need normalization here, can directly query the Gateway

        # 1) Fetch the Storage's announces and get the corresponding Tables.
        local_announces = self.get_announces()
        local_tables = frozenset([announce.get_table() for announce in local_announces])

        # 2) Build the corresponding map of Capabilities
        map_method_capabilities = dict()
        for announce in local_announces:
            table = announce.get_table()
            platform_names = table.get_platforms()
            assert len(platform_names) == 1, "An announce should be always related to a single origin"
            table_name = table.get_name()
            platform_name = iter(platform_names).next()
            method = Method(platform_name, table_name)
            capabilities = table.get_capabilities()
            map_method_capabilities[method] = capabilities

        # 3) Build the corresponding DBGraph
        return DBGraph(local_tables, map_method_capabilities)

