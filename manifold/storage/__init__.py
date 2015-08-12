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
from manifold.core.address          import Address
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

    def __init__(self, router, platform_name = 'storage', **platform_config):
        """
        Constructor.
        Args:
            router: The Router configured by this Storage.
            platform_name: A String identifying the Storage among the
               other Platforms managed by the Router.
            platform_config: A dict storing the configuration of the
               Storage.
        """
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

        self.register_model_collection(ModelAccount,       'account',        LOCAL_NAMESPACE)
        self.register_model_collection(ModelLinkedAccount, 'linked_account', LOCAL_NAMESPACE)
        self.register_model_collection(ModelPlatform,      'platform',       LOCAL_NAMESPACE)
        self.register_model_collection(ModelPolicy,        'policy',         LOCAL_NAMESPACE)
        self.register_model_collection(ModelSession,       'session',        LOCAL_NAMESPACE)
        self.register_model_collection(ModelUser,          'user',           LOCAL_NAMESPACE)

        self._update_router()

    def _update_router(self):
        """
        Enable in the nested router the Interfaces related to the enabled Platforms.
        By doing so, Router class do not depend on Storage (the Storage configures
        the Router).
        """
        # XXX This code should be factored to be used anywhere we need to make a
        # query.
        # Redundant with : Router::execute_query()
        platform_collection = self.get_collection('platform', LOCAL_NAMESPACE)
        packet = GET()
        destination = Address('platform')

        # This should be automatically done by send based on the destination
        # address
        packet.set_destination(destination)
        packet.set_source(self.get_address()) 
        receiver = SyncReceiver()
        packet.set_receiver(receiver)
        platforms = platform_collection.get(packet)

        # This code is blocking
        result_value = receiver.get_result_value()
        platforms = result_value.get_all().to_dict_list()
        for platform in platforms:
            platform_config = json.loads(platform['config'])
            try:
                # Load only the enabled platforms
                if platform['disabled'] == 0:
                    self._router.add_interface(platform['gateway_type'], platform['platform'], **platform_config)
            except Exception, e:
                import traceback
                traceback.print_exc()
                Log.error(e)

    # Storage is always up
    def is_up(self):
        return True
