#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Configure default storage for most of the manifold
# commands provided in manifold.bin
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from manifold.util.storage.sqla_storage import SQLAlchemyStorage

STORAGE_FILENAME = "/var/myslice/db.sqlite"
STORAGE_URL      = "sqlite:///%s?check_same_thread=False" % STORAGE_FILENAME

STORAGE_CONFIG   = {
    "url"  : STORAGE_URL,
    "user" : None
}

MANIFOLD_STORAGE = SQLAlchemyStorage(
    platform_config = STORAGE_CONFIG 
)
