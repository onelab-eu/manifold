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

from manifold.core.record           import Records
from manifold.gateways              import Gateway, OLocalLocalColumn
from manifold.gateways.object       import ManifoldObject
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns


LOCAL_NAMESPACE = "local"


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

    def get(self, query = None):
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
        return Records([{"type" : gateway_type} for gateway_type in sorted(Gateway.facotry_list().keys())])

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

        # XXX We could automatically load objects...
        self.register_object(OLocalObject)
        self.register_object(OLocalColumn)
        self.register_object(OLocalGateway)


