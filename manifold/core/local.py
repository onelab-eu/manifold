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
        namespace = query.get_namespace()
        table_name = query.get_table_name()

        if namespace == LOCAL_NAMESPACE:
            if table_name == 'object':
                announces = make_local_announces(LOCAL_NAMESPACE)
                records = Records([x.to_dict() for x in announces])
            else:
                raise RuntimeError("Invalid object '%s::%s'" % (namespace, object_name))
        else:
            if table_name == "object":
                if not action == "get":
                     raise RuntimeError("Invalid action (%s) on '%s::%s' table" % (action, self.get_platform_name(), table_name))
                records = Records([a.to_dict() for a in self._router.get_fib().get_announces()]) # only default namespace for now
            elif table_name == "gateway":
                # Note that local:column won't be queried since it has no RETRIEVE capability.
                if not action == "get":
                     raise RuntimeError("Invalid action (%s) on '%s::%s' table" % (action, self.get_platform_name(), table_name))
                records = Records([{"type" : gateway_type} for gateway_type in sorted(Gateway.list().keys())])
            elif self._storage:
                records = None
                self._storage.get_gateway().receive_impl(packet)
            else:
                raise RuntimeError("Invalid object '%s::%s'" % (namespace, object_name))

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

# DEPRECATED BY FIB    @returns(DBGraph)
# DEPRECATED BY FIB    def make_dbgraph(self):
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        Make the DBGraph.
# DEPRECATED BY FIB        Returns:
# DEPRECATED BY FIB            The DBGraph related to the Manifold Storage.
# DEPRECATED BY FIB        """
# DEPRECATED BY FIB        # We do not need normalization here, can directly query the Gateway
# DEPRECATED BY FIB
# DEPRECATED BY FIB        # 1) Fetch the Storage's announces and get the corresponding Tables.
# DEPRECATED BY FIB        local_announces = self.get_announces()
# DEPRECATED BY FIB        local_tables = frozenset([announce.get_table() for announce in local_announces])
# DEPRECATED BY FIB
# DEPRECATED BY FIB        # 2) Build the corresponding map of Capabilities
# DEPRECATED BY FIB        map_method_capabilities = dict()
# DEPRECATED BY FIB        for announce in local_announces:
# DEPRECATED BY FIB            table = announce.get_table()
# DEPRECATED BY FIB            platform_names = table.get_platforms()
# DEPRECATED BY FIB            assert len(platform_names) == 1, "An announce should be always related to a single origin"
# DEPRECATED BY FIB            table_name = table.get_name()
# DEPRECATED BY FIB            platform_name = iter(platform_names).next()
# DEPRECATED BY FIB            method = Method(platform_name, table_name)
# DEPRECATED BY FIB            capabilities = table.get_capabilities()
# DEPRECATED BY FIB            map_method_capabilities[method] = capabilities
# DEPRECATED BY FIB
# DEPRECATED BY FIB        # 3) Build the corresponding DBGraph
# DEPRECATED BY FIB        return DBGraph(local_tables, map_method_capabilities)

