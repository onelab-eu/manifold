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

from manifold                       import __version__
from manifold.core.record           import Records
from manifold.gateways              import Gateway, OLocalLocalColumn
from manifold.gateways.object       import ManifoldCollection
from manifold.util.filesystem       import hostname
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns


LOCAL_NAMESPACE = "local"


class OLocalObject(ManifoldCollection):
    """
    class object {
        string  table;           /**< The name of the object/table.        */
        column  columns[];       /**< The corresponding fields/columns.    */
        string  capabilities[];  /**< The supported capabilities           */
        string  key[];           /**< The keys related to this object      */
        string  origins[];       /**< The platform originating this object */
        string platforms[];     /**< The next_hops advertising this object */

        CAPABILITY(retrieve);
        KEY(table);
    };
    """

    def get(self, query = None):
        object_list = list()
        for obj in self.get_router().get_fib().get_announces():
            obj_dict = obj.to_dict()

            object_name = obj_dict['table']
            fib = self.get_router().get_fib()
            
            obj_dict['platforms'] = fib.get_object(object_name).get_platform_names()

            object_list.append(obj_dict)
        return Records(object_list)

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
        CAPABILITY(retrieve);
        KEY(name);
    };
    """
    def get(self, packet):
        interface_list = list()
        for interface_name, interface in self.get_router().get_interfaces():
            interface = {
                'name': interface_name, 
                'type': interface.get_interface_type()
            }
            interface_list.append(interface)
        return Records(interface_list)

class LocalAboutCollection(ManifoldCollection):
    """
    class about {
        string hostname;
        string version;
    };
    """
    def get(self, packet):
        about = {
            'hostname': hostname(),
            'version':  __version__
        }
        return Records([about])


# LocalGateway should be a standard gateway to which we register objects
# No need for a separate class
class LocalGateway(Gateway):
    """
    Handle queries related to local:object, local:gateway, local:platform, etc.
    """

    def __init__(self, router = None, platform_name = 'local', platform_config = None):
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

        super(LocalGateway, self).__init__(router, platform_name, platform_config)

        # Overwrite default collections
        self.register_collection(OLocalObject(), 'local')
        self.register_collection(OLocalColumn(), 'local')

        self.register_collection(LocalGatewayCollection(), 'local')
        self.register_collection(LocalInterfaceCollection(), 'local')
        self.register_collection(LocalAboutCollection(), 'local')


