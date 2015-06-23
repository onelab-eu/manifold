#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Contains all the constants related to Manifold components.
# CONSTANTS MUST BE STRINGS (they can be used as default Options).
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import json

#----------------------------------------------------------------------
# User-defined constants
#----------------------------------------------------------------------

# Manifold router
SOCKET_PATH             = "/var/run/manifold/manifold.sock"

#----------------------------------------------------------------------
# Manifold peers
#----------------------------------------------------------------------

DEFAULT_PEER_URL        = "http://%(hostname)s:%(port)s/RPC/"
DEFAULT_PEER_PORT       = 58000

#----------------------------------------------------------------------
# Manifold static routes 
#----------------------------------------------------------------------

STATIC_ROUTES_DIR = "/usr/share/manifold/metadata/"

