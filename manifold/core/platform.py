#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# See manifold/models/platform.py
#
#DEPRECATED|# A Platform represents a source of data. A Platform is related
#DEPRECATED|# to a Gateway, which wrap this source of data in the Manifold
#DEPRECATED|# framework. For instance, TDMI is a Platform using the PostgreSQL
#DEPRECATED|# Gateway.
#DEPRECATED|#
#DEPRECATED|# Copyright (C) UPMC Paris Universitas
#DEPRECATED|# Authors:
#DEPRECATED|#   Jordan Augé       <jordan.auge@lip6.fr>
#DEPRECATED|#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#DEPRECATED|
#DEPRECATED|from types                 import StringTypes
#DEPRECATED|from manifold.util.type    import returns, accepts
#DEPRECATED|
#DEPRECATED|class Platform(object):
#DEPRECATED|    def __init__(self, name, gateway_name, gateway_config, auth_type):
#DEPRECATED|        """
#DEPRECATED|        Create a Platform instance.
#DEPRECATED|        Args:
#DEPRECATED|            name: The platform name (String instance) 
#DEPRECATED|            gateway_name: The platform type (String instance)
#DEPRECATED|            gateway_config: A dictionnary containing the Gateway configuration, for
#DEPRECATED|                example the default credentials used to connect to the Gateway.
#DEPRECATED|            auth_type: A String instance (for instance "user", "none", "default", "reference") 
#DEPRECATED|        """
#DEPRECATED|        self.name           = name
#DEPRECATED|        self.gateway_name   = gateway_name
#DEPRECATED|        self.gateway_config = gateway_config
#DEPRECATED|        self.auth_type      = auth_type
#DEPRECATED|
#DEPRECATED|    @returns(StringTypes)
#DEPRECATED|    def __str__(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The %s representation of a Platform instance.
#DEPRECATED|        """
#DEPRECATED|        return "<Platform %s (%s [%r])>" % (self.name, self.gateway_name, self.gateway_config)
#DEPRECATED|
#DEPRECATED|    @returns(StringTypes)
#DEPRECATED|    def __repr__(self):
#DEPRECATED|        """
#DEPRECATED|        Returns:
#DEPRECATED|            The %r representation of a Platform instance.
#DEPRECATED|        """
#DEPRECATED|        return str(self)
#DEPRECATED|
#DEPRECATED|
