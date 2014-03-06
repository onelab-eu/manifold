#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Configure default storage for most of the manifold
# commands provided in manifold.bin
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import traceback

try:
    from manifold.util.storage.sqla_storage import SQLAlchemyStorage

    STORAGE_FILENAME = "/var/myslice/db.sqlite"
    STORAGE_URL      = "sqlite:///%s?check_same_thread=False" % STORAGE_FILENAME

    STORAGE_CONFIG   = {
        "url"  : STORAGE_URL,
        "user" : None
    }

    # This trigger Options parsing because Gateway.register_all() uses Logging
    MANIFOLD_STORAGE = SQLAlchemyStorage(
        platform_config = STORAGE_CONFIG 
    )
except Exception, e:
    from manifold.util.log import Log

    Log.warning("Running Manifold without Storage due to the following Exception")
    Log.warning("%s" % traceback.format_exc())
    MANIFOLD_STORAGE = None
