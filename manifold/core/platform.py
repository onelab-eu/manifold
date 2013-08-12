#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Platform module
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                 import StringTypes
from manifold.util.type    import returns, accepts

class Platform(object):
    def __init__(self, name, gateway_name, gateway_config, auth_type):
        """
        Create a Platform instance
        Args:
            name: The platform name (String instance) 
            gateway_name: The platform type (String instance)
            gateway_config: A dictionnary containing the Gateway configuration, for
                example the credentials used to connnect to the Gateway.
            auth_type:     
        """
        self.name = name
        self.gateway_name = gateway_name
        self.gateway_config = gateway_config
        self.auth_type = auth_type

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The %s representation of a Platform instance.
        """
        return "<Platform %s (%s [%r])>" % (self.name, self.gateway_name, self.gateway_config)

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The %r representation of a Platform instance.
        """
        return str(self)
