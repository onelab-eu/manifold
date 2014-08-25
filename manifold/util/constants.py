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

# Storage
# - STORAGE_DEFAULT_GATEWAY:
#   It must be a String corresponding to a __gateway_name__ (see manifold/gateway/*/__init__.py)

STORAGE_SQLA_FILENAME   = "/var/lib/manifold/storage.sqlite"
STORAGE_SQLA_USER       = None
STORAGE_SQLA_PASSWORD   = None
STORAGE_SQLA_URL        = "sqlite:///%s?check_same_thread=False" % STORAGE_SQLA_FILENAME

STORAGE_SQLA_CONFIG     = json.dumps({
    "url"      : STORAGE_SQLA_URL
})

STORAGE_SQLA_ANNOTATION = {
    "user"     : STORAGE_SQLA_USER,
    "password" : STORAGE_SQLA_PASSWORD
}

STORAGE_DEFAULT_GATEWAY    = "sqlalchemy"
STORAGE_DEFAULT_CONFIG     = STORAGE_SQLA_CONFIG
STORAGE_DEFAULT_ANNOTATION = STORAGE_SQLA_ANNOTATION

# Manifold peers
DEFAULT_PEER_URL        = "http://%(hostname)s:%(port)s/RPC/"
DEFAULT_PEER_PORT       = 58000
