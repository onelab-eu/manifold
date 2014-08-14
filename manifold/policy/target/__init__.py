#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Target support for the Manifold policy engine.
#
# This file is part of the Manifold project
#
# Copyright (C)2009-2013, UPMC Paris Universitas
# Authors:
#   Jordan Augé <jordan.auge@lip6.fr>
#   Loïc Baron  <loic.baron@lip6.fr>

from manifold.util.log                  import Log

class TargetValue(object):
    # This is inspired from Netfilter

    # Continue evaluating rules
    ACCEPT      = 0
    REWRITE     = 1
    CACHE_HIT   = 2
    DENIED      = 3
    ERROR       = 4
    CONTINUE    = 5

class Target(object):
    """
    Base object for implementing targets.
    """

    map_targets = {}

    def __init__(self, interface):
        self._interface = interface

    @staticmethod
    def register(name, cls):
        Target.map_targets[name] = cls

    @staticmethod
    def get(name):
        return Target.map_targets.get(name, None)

    @staticmethod
    def register_plugins():
        from manifold.policy.target.drop  import DropTarget
        from manifold.policy.target.log   import LogTarget
        from manifold.policy.target.cache import CacheTarget
        Target.register('DROP', DropTarget)
        Target.register('LOG',  LogTarget)
        Target.register('CACHE',  CacheTarget)
