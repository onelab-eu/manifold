#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This Gateway allows to access to SFA (Slice Federated
# Architecture) 
# http://groups.geni.net/geni/wiki/GeniApi
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
# Amine Larabi      <mohamed.larabi@inria.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

from types                              import StringTypes
from twisted.internet                   import defer
from manifold.util.type                 import accepts, returns

class Object:
    aliases = dict()

    def __init__(self):
        pass

    @staticmethod
    @returns(StringTypes)
    def get_alias(field_name):
        """
        Args:
            field_name: The name of a field related to an SFA object
        """
        assert isinstance(field_name, StringTypes), "Invalid field name: %s (%s)" % (field_name, type(field_name))
        return Object.aliases[field_name]


