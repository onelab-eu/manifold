#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Configure default storage for most of the manifold
# commands provided in manifold.bin
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import json, traceback
from ..util.constants   import STORAGE_DEFAULT_GATEWAY, STORAGE_DEFAULT_CONFIG

def make_storage(storage_gateway, storage_config):
    """
    Args:
        storage_gateway: The Gateway used to contact the Storage.
        storage_config: A json-ed String containing the Gateway configuration.
    Returns:
        The corresponding Storage.
    """
    if STORAGE_DEFAULT_GATEWAY == "sqlalchemy":
        from manifold.util.storage.sqlalchemy.sqla_storage import SQLAlchemyStorage

        # This trigger Options parsing because Gateway.register_all() uses Logging
        return SQLAlchemyStorage(
            platform_config = storage_config
        )
    else:
        raise ValueError("Invalid STORAGE_DEFAULT_GATEWAY constant (%s)" % STORAGE_DEFAULT_GATEWAY)

try:
    MANIFOLD_STORAGE = make_storage(
        STORAGE_DEFAULT_GATEWAY,
        json.loads(STORAGE_DEFAULT_CONFIG)
    )
except Exception, e:
    from manifold.util.log import Log

    Log.warning("Running Manifold without Storage due to the following Exception")
    Log.warning("%s" % traceback.format_exc())
    MANIFOLD_STORAGE = None
