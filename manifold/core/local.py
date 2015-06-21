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

import time
from datetime                       import timedelta
from types                          import StringTypes

from manifold                       import __version__
from manifold.core.record           import Records
from manifold.gateways              import Gateway, OLocalLocalColumn
from manifold.gateways.object       import ManifoldCollection
from manifold.util.filesystem       import hostname
from manifold.util.log              import Log
from manifold.util.misc             import strfdelta
from manifold.util.type             import accepts, returns

LOCAL_NAMESPACE = "local"
UPTIME_STRING = "{days} days {hours}:{minutes}:{seconds}"

class OLocalObject(ManifoldCollection):
    """
    class object {
        string object_name;     /**< The name of the object/table.        */
        string namespace;       
        column columns[];       /**< The corresponding fields/columns.    */
        string capabilities[];  /**< The supported capabilities           */
        string key[];           /**< The keys related to this object      */
        string origins[];       /**< The platform originating this object */
        string partitions[];
        string platforms[];     /**< The next_hops advertising this object */

        CAPABILITY(retrieve);
        KEY(object_name);
    };
    """

    def get(self, packet):
        destination = packet.get_destination()
        namespace = destination.get_namespace()
        table_list = list()
        for obj in self.get_router().get_fib().get_objects(namespace='*'):
            announce = obj.get_announce()
            table = announce.get_table()
            if table.get_namespace() == 'local':
                continue
            table_dict = table.to_dict()
            table_dict['platforms'] = obj.get_platform_names()
            table_dict['columns'] = Records(table_dict['columns'])

            table_list.append(table_dict)

        records = Records(table_list)
        return records

OLocalColumn = OLocalLocalColumn

class LocalGatewayCollection(ManifoldCollection):
    """
    class gateway {
        string type;

        CAPABILITY(retrieve);
        KEY(type);
    };
    """
    def get(self, packet):
        gateway_list = list()
        for gateway_type in sorted(Gateway.factory_list().keys()):
            gateway = {'type': gateway_type}
            gateway_list.append(gateway)
        return Records(gateway_list)

class LocalInterfaceCollection(ManifoldCollection):
    """
    class interface {
        string name;
        string type;
        string status;
        string description;
        CAPABILITY(retrieve);
        KEY(name);
    };
    """
    def get(self, packet):
        interface_list = list()
        for interface_name, interface in self.get_router().get_interfaces():
            interface = {
                'name': interface_name, 
                'type': interface.get_interface_type(),
                'status': interface.get_status(),
                'description': interface.get_description(),
            }
            interface_list.append(interface)
        return Records(interface_list)

    def create(self, packet):
        router = self.get_gateway().get_router()
        data = packet.get_data()
        interface_type = data.pop('type')
        try:
            import pdb; pdb.set_trace()
            interface = router.add_interface(interface_type, None, **data)
        except Exception, e:
            print "TODO: send error packet", e
            return
        interface.set_up()

class LocalAboutCollection(ManifoldCollection):
    # XXX Metadata will vary with time, how to refresh ?? how to describe ??
    """
    class about {
        string hostname;
        string version;
        CAPABILITY(retrieve);
        KEY(hostname);
    };
    """
    def get(self, packet):
        about = {
            'hostname': hostname(),
            'version':  __version__
        }
        router_keyvalues = self.get_gateway().get_router().get_keyvalue_store()

        # XXX This should be done in agent
        about.update(router_keyvalues)
        if 'agent_started' in about:
            uptime = time.time() - about['agent_started']
            about['agent_uptime'] = uptime
            about['agent_uptime_human'] = strfdelta(timedelta(seconds=uptime), UPTIME_STRING)
        if 'agent_upernode_started' in about:
            uptime = time.time() - about['agent_supernode_started']
            about['agent_supernode_uptime'] = uptime
            about['agent_supernode_uptime_human'] = strfdelta(timedelta(seconds=uptime), UPTIME_STRING)
        return Records([about])


# LocalGateway should be a standard gateway to which we register objects
# No need for a separate class
class LocalGateway(Gateway):
    """
    Handle queries related to local:object, local:gateway, local:platform, etc.
    """

    def __init__(self, router = None, platform_name = 'local', **platform_config):
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

        super(LocalGateway, self).__init__(router, platform_name, **platform_config)

        # Overwrite default collections
        self.register_collection(OLocalObject(), 'local')
        self.register_collection(OLocalColumn(), 'local')

        self.register_collection(LocalGatewayCollection(), 'local')
        self.register_collection(LocalInterfaceCollection(), 'local')
        self.register_collection(LocalAboutCollection(), 'local')
        self.register_collection(LocalAboutCollection(), 'global')


