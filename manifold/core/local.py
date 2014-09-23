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
from manifold.core.announce         import Announce, Announces, parse_string, announces_from_docstring
from manifold.core.dbgraph          import DBGraph
from manifold.core.method           import Method
from manifold.core.record           import Record, Records
from manifold.core.table            import Table
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

LOCAL_NAMESPACE = "local"

class ManifoldObject(Record):
    __object_name__ = None
    __fields__      = None
    __keys__        = None
    _collection     = list()

    def get_router(self):
        return self.get_gateway().get_router()

    def get_gateway(self):
        return self._gateway

    def set_gateway(self, gateway):
        self._gateway = gateway

    @classmethod
    def get_object_name(cls):
        if cls.__doc__:
            announce = cls.get_announce()
            return announce.get_table().get_name()
        else:
            return cls.__object_name__ if cls.__object_name__ else cls.__name__

    @classmethod
    def get_fields(cls):
        if cls.__doc__:
            announce = self.get_announce()
            return announce.get_table().get_fields()
        else:
            return cls.__fields__

    @classmethod
    def get_keys(cls):
        if cls.__doc__:
            announce = self.get_announce()
            return announce.get_table().get_keys()
        else:
            return cls.__keys__

    @classmethod
    def get(cls, filter = None, fields = None):
        import copy
        ret = list()
        # XXX filter and fields
        # XXX How to preserve the object class ?
        for x in cls._collection:
            y = copy.deepcopy(x)
            y.__class__ = Record
            ret.append(y)
        return ret

    def insert(self):
        self._collection.append(self)

    def remove(self):
        self._collection.remove(self)

    @classmethod
    def get_announce(cls):
        # The None value corresponds to platform_name. Should be deprecated # soon.
        if cls.__doc__:
            announce, = parse_string(cls.__doc__, None)
            return announce
        else:
            table = Table(None, cls.get_object_name(), cls.get_fields(), cls.get_keys())
            #table.set_capability()
            #table.partitions.append()
            return Announce(table)

# The sets of objects exposed by this gateway
class OLocalLocalObject(ManifoldObject):
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
    """

    def get(self):
        return Records([cls.get_announce().to_dict() for cls in self.get_gateway().get_objects()])

class OLocalLocalColumn(ManifoldObject):
    """
    class column {
        string qualifier;
        string name;
        string type;
        string description;
        bool   is_array;

        LOCAL KEY(name);
    };
    """

class OLocalObject(ManifoldObject):
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
    """

    def get(self):
        return Records([a.to_dict() for a in self.get_router().get_fib().get_announces()]) # only default namespace for now

OLocalColumn = OLocalLocalColumn

class OLocalGateway(ManifoldObject):
    """
    class gateway {
        string type;

        CAPABILITY(retrieve);
        KEY(type);
    };
    """
    def get(self):
        return Records([{"type" : gateway_type} for gateway_type in sorted(Gateway.list().keys())])

# LocalGateway should be a standard gateway to which we register objects
# No need for a separate class
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
        # namespace -> (object_name -> obj)
        self._objects_by_namespace = dict()

        # XXX We don't need self.storage anymore

        try:
            from manifold.bin.config import MANIFOLD_STORAGE
            self._storage = MANIFOLD_STORAGE
        except Exception, e:
            Log.warning(traceback.format_exc())
            Log.warning("LocalGateway: cannot load Storage.")
            self._storage = None

        super(LocalGateway, self).__init__(router, platform_name, platform_config)

        self.register_local_object(OLocalLocalObject)
        self.register_local_object(OLocalLocalColumn)
        self.register_object(OLocalObject)
        self.register_object(OLocalColumn)
        self.register_object(OLocalGateway)


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
        object_name = query.get_object_name()

        try:
            obj = self.get_object(object_name, namespace)
        except ValueError:
            raise RuntimeError("Invalid object '%s::%s'" % (namespace, object_name))

        instance = obj()
        instance.set_gateway(self)
        records = instance.get()

        #elif self._storage:
        #    records = None
        #    self._storage.get_gateway().receive_impl(packet)

        if records:
            self.records(records, packet)

    def get_object(self, object_name, namespace = None):
        return self._objects_by_namespace[namespace][object_name]

    def get_objects(self, namespace = None):
        return self._objects_by_namespace[namespace].values()

    def register_object(self, cls, namespace = None, is_local = False):
        # Register it in the FIB
        self.get_router().get_fib().add('local', cls.get_announce(), namespace)

        # If the addition request does not come locally, then we don't need to
        # keep the namespace (usually 'local')
        if not is_local:
            namespace = None

        # Store the object locally
        if namespace not in self._objects_by_namespace:
            self._objects_by_namespace[namespace] = dict()
        self._objects_by_namespace[namespace][cls.get_object_name()] = cls

    def register_local_object(self, cls, namespace = LOCAL_NAMESPACE):
        self.register_object(cls, namespace, is_local = True)

#DEPRECATED|        # Fetch Announces produced by the Storage
#DEPRECATED|        gateway_storage = self._storage.get_gateway()
#DEPRECATED|        if gateway_storage:
#DEPRECATED|            #local_announces = local_announces | gateway_storage.get_announces()
#DEPRECATED|            local_announces |= gateway_storage.get_announces()
#DEPRECATED|
#DEPRECATED|        # Fetch Announces produced by each enabled platform.
#DEPRECATED|        router = self.get_router()
#DEPRECATED|        if router:
#DEPRECATED|            for platform_name in router.get_enabled_platform_names():
#DEPRECATED|                gateway = router.get_gateway(platform_name)
#DEPRECATED|                # foo:object is renamed local:object since we cannot compute query plan
#DEPRECATED|                # over the local DBGraph if its table are not attached to platform "local"
#DEPRECATED|                local_announces |= make_local_announces(LOCAL_NAMESPACE)
#DEPRECATED|        else:
#DEPRECATED|            Log.warning("The router of this %s is unset. Some Announces cannot be fetched" % self)
#DEPRECATED|
#DEPRECATED|        return local_announces

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

