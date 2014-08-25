#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Target support for the Manifold policy engine.
#
# This file is part of the Manifold project
#
# Copyright (C)2009-2013, UPMC Paris Universitas
# Authors:
#   Jordan Augé      <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

class TargetValue(object):
    # This is inspired from Netfilter

    # Continue evaluating rules
    ACCEPT   = 0
    REWRITE  = 1
    RECORDS  = 2
    DENIED   = 3
    ERROR    = 4
    CONTINUE = 5

class Target(object):
    """
    Base object for implementing targets.
    """

    map_targets = dict()

    @staticmethod
    def register(name, cls):
        """
        Register a Target in Manifold.
        Args:
            name: A String corresponding to a Target.
            cls:  A Target class (which shoud be in manifold/policy/target)
        """
        Target.map_targets[name] = cls

    @staticmethod
    def get(name):
        return Target.map_targets.get(name, None)

    @staticmethod
    def register_plugins():
        from ..target.drop  import DropTarget
        from ..target.log   import LogTarget
        from ..target.cache import CacheTarget
        Target.register("DROP",  DropTarget)
        Target.register("LOG",   LogTarget)
        Target.register("CACHE", CacheTarget)
