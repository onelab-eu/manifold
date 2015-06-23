#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# The Manifold Storage stores the Manifold configuration, including
# the Manifold users, accounts, and platforms.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.f>

import json, os.path, traceback
from types                          import StringTypes

from manifold.gateways              import Gateway
from manifold.core.annotation       import Annotation
from manifold.core.packet           import GET
from manifold.core.query            import Query
from manifold.core.local            import LOCAL_NAMESPACE
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.filesystem       import ensure_writable_directory, check_readable_file
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

from manifold.gateways.sqlalchemy   import SQLAlchemyCollection, SQLAlchemyGateway

STORAGE_SQLA_FILENAME   = "/var/lib/manifold/storage.sqlite"
STORAGE_SQLA_USER       = None
STORAGE_SQLA_PASSWORD   = None
STORAGE_SQLA_URL        = "sqlite:///%s?check_same_thread=False" % STORAGE_SQLA_FILENAME

STORAGE_SQLA_CONFIG     = json.dumps({
    "url"      : STORAGE_SQLA_URL
})

# XXX Not used at the moment
# XXX Need a way to automatically add an annotation to a Storage query when
# forwarded to SQLAlchemy
STORAGE_SQLA_ANNOTATION = {
    "user"     : STORAGE_SQLA_USER,
    "password" : STORAGE_SQLA_PASSWORD
}

class StorageCollection(SQLAlchemyCollection):
    pass

class StorageGateway(SQLAlchemyGateway):

    def __init__(self, router, platform_name='storage', **platform_config):
        url = platform_config.get('url', STORAGE_SQLA_URL)
        platform_config['url'] = url
        db_filename = os.sep.join(url.split('?')[0].split('/')[1:])
        # XXX The check for filename before dirname is weird...
        check_readable_file(db_filename)
        db_dirname = os.path.dirname(db_filename)
        ensure_writable_directory(db_dirname)

        SQLAlchemyGateway.__init__(self, router, 'storage', **platform_config)

        from .models.account               import ModelAccount
        from .models.linked_account        import ModelLinkedAccount
        from .models.platform              import ModelPlatform
        from .models.policy                import ModelPolicy
        from .models.session               import ModelSession
        from .models.user                  import ModelUser

        self.register_model_collection(ModelAccount, 'account', 'local')
        self.register_model_collection(ModelLinkedAccount, 'linked_account', 'local')
        self.register_model_collection(ModelPlatform, 'platform', 'local')
        self.register_model_collection(ModelPolicy, 'policy', 'local')
        self.register_model_collection(ModelSession, 'session', 'local')
        self.register_model_collection(ModelUser, 'user', 'local')
