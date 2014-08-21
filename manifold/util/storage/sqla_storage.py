#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# An implementation of the Manifold Storage using
# SQLAlchemy and sqlite.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import os
from types                          import StringTypes

from manifold.core.annotation       import Annotation
from manifold.util.filesystem       import ensure_writable_directory, check_readable_file
from manifold.util.type             import returns
from ..storage.storage              import Storage

class SQLAlchemyStorage(Storage):

    def __init__(self, platform_config = None, router = None):
        """
        Constructor.
        Args:
            platform_config: A dictionnary containing the relevant information
                to instantiate the corresponding Gateway.
            router: The Router on which this Storage is running.
                You may pass None if this Storage is stand-alone.
        """

        url = platform_config["url"]
        db_filename = os.sep.join(url.split('?')[0].split('/')[1:])
        check_readable_file(db_filename)

        db_dirname = os.path.dirname(db_filename)
        ensure_writable_directory(db_dirname)

        super(SQLAlchemyStorage, self).__init__("sqlalchemy", platform_config, router)

        self._storage_annotation = Annotation({
            "user" : platform_config.get("user", None)
        })
