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

class TargetValue(object):
    # This is inspired from Netfilter

    # Continue evaluating rules
    CONTINUE = 0

    # Drop the query and stop evaluating rules
    DROP = 1

    # The query is stolen from the normal path
    STOLEN = 2

    # The query is queued for further processing
    QUEUE = 3

    # Restart rule processing
    REPEAT = 4

class Target(object):
    """
    Base object for implementing targets.
    """

    map_targets = {}

    @staticmethod
    def register(name, cls):
        Target.map_targets[name] = cls

    @staticmethod
    def get(name):
        return Target.map_targets[name]

    @staticmethod
    def register_plugins():
        from manifold.policy.target.drop import DropTarget
        from manifold.policy.target.log  import LogTarget
        Target.register('DROP', DropTarget)
        Target.register('LOG',  LogTarget)
