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
from manifold.util.filesystem       import ensure_writable_directory
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

class Storage(object):
    def __init__(self, gateway_type, platform_config, router = None):
        """
        Constructor.
        Args:
            gateway_type: A String containing the type of Gateway used to
                query this Manifold Storage. Example: "sqlalchemy"
            platform_config: A dictionnary containing the relevant information
                to instantiate the corresponding Gateway.
            router: The Router on which this Storage is running.
                You may pass None if this Storage is stand-alone.
        """
        assert isinstance(gateway_type, StringTypes),\
            "Invalid gateway_type = %s (%s)" % (gateway_type, type(gateway_type))
        assert isinstance(platform_config, dict),\
            "Invalid platform_config = %s (%s)" % (platform_config, type(platform_config))

        assert router

        self._gateway         = None
        self._gateway_type    = gateway_type
        self._platform_config = platform_config
        self._router       = router

    def load_gateway(self):
        # Initialize self._gateway
        Gateway.register_all()
        cls_storage = Gateway.get(self._gateway_type)
        if not cls_storage:
            raise RuntimeError("Cannot find %s Gateway, required to access Manifold Storage" % gateway_type)
        self._gateway = cls_storage(self._router, LOCAL_NAMESPACE, self._platform_config)

    @returns(Gateway)
    def get_gateway(self):
        """
        Returns:
            The Gateway nested in this Storage instancec.
        """
        if not self._gateway:
            self.load_gateway()
        return self._gateway

    @returns(list)
    def execute(self, query, annotation = None, error_message = None):
        """
        Execute a Query related to the Manifold Storage
        (ie any "LOCAL_NAMESPACE:*" object).
        Args:
            gateway: A Gateway instance allowing to query the Manifold Storage.
            query: A Query. query.get_from() should start with "local:".
            annotation: An Annotation instance related to Query or None.
            error_message: A String containing the error_message that must
                be written in case of failure.
        Raises:
            Exception: if the Query does not succeed.
        Returns:
            A list of Records.
        """
        gateway = self.get_gateway()
        namespace = query.get_namespace()

        # Check parameters
        assert gateway and isinstance(gateway, Gateway),\
            "Invalid gateway = %s (%s)" % (gateway, type(gateway))
        assert not annotation or isinstance(annotation, Annotation),\
            "Invalid annotation = %s (%s)" % (annotation, type(annotation))
        assert namespace in [None, LOCAL_NAMESPACE],\
            "Invalid namespace: '%s' != '%s'" % (query.get_from(), LOCAL_NAMESPACE)

        # Prepare the Receiver and the QUERY Packet
        receiver = SyncReceiver()
        packet = GET()
        packet.set_query(query)
        packet.set_receiver(receiver)

        # Send the Packet and collect the corresponding RECORD Packets.
        gateway.receive(packet)
        result_value = receiver.get_result_value()

        # In case of error, raise an error.
        if not result_value.is_success():
            if not error_message:
                error_message = "Error executing local query: %s" % query
            raise RuntimeError(error_message)

        # Otherwise, return the corresponding list of dicts.
        return [record.to_dict() for record in result_value["value"]]

    def update_router_state(self, router, annotation, platform_names = None):
        """
        Update the router state in respect with the Storage content.
        Args:
            router: A Router instance able to contact the Storage.
            annotation: An Annotation instance, which typically transports
                credential to access to the Storage.
            platform_names: A set/frozenset of String where each String
                is the name of a Platform. If you pass None,
                all the Platform not disabled in the Storage
                are considered.
        """
        ERR_CANNOT_LOAD_STORAGE = "While executing %(query)s: Cannot load storage."

        assert isinstance(annotation, Annotation),\
            "Invalid annotation = %s (%s)" % (annotation, type(annotation))
        assert not platform_names or isinstance(platform_names, (frozenset, set)),\
            "Invalid platform_names = %s (%s)" % (platform_names, type(platform_names))

        # Register ALL the platform configured in the Storage.
        query = Query.get("platform")
        platforms_storage = self.execute(query, annotation, ERR_CANNOT_LOAD_STORAGE)
        for platform in platforms_storage:
            try:
                if platform["config"]:
                    platform["config"] = json.loads(platform["config"])
                else:
                    platform["config"] = dict()
            except ValueError:
                Log.warning("Storage: platform %s has an invalid configuration and will be ignored (not json): %s" %
                    (
                        platform["platform"],
                        platform["config"]
                    )
                )
                continue
            router.register_platform(platform)

        # Fetch enabled Platforms from the Storage...
        if not platform_names:
            query = Query.get("platform")\
                .select("platform")\
                .filter_by("disabled", "==", False)
            platforms_storage = self.execute(query, annotation, ERR_CANNOT_LOAD_STORAGE)
        else:
            query = Query.get("platform")\
                .select("platform")\
                .filter_by("platform", "INCLUDED", platform_names)
            platforms_storage = self.execute(query, annotation, ERR_CANNOT_LOAD_STORAGE)

            # Check whether if all the requested Platforms have been found in the Storage.
            platform_names_storage = set([platform["platform"] for platform in platforms_storage])
            platform_names_missing = platform_names - platform_names_storage
            if platform_names_missing:
                Log.warning("The following platform names are undefined in the Storage: %s" % platform_names_missing)

        # Load/unload platforms managed by the router (and their corresponding Gateways) consequently
        router.update_platforms(platforms_storage)

        # Load policies from Storage
        query_rules = Query.get("policy")\
            .select("policy_json")
        rules = self.execute(query_rules, annotation, "Cannot load policy from storage")
        for rule in rules:
            router.policy.add_rule(rule)

def make_storage(storage_gateway, storage_config, router):
    """
    Args:
        storage_gateway: The Gateway used to contact the Storage.
        storage_config: A json-ed String containing the Gateway configuration.
    Returns:
        The corresponding Storage.
    """
    from manifold.util.constants import STORAGE_DEFAULT_GATEWAY

    if STORAGE_DEFAULT_GATEWAY == "sqlalchemy":
        from manifold.util.storage.sqlalchemy.sqla_storage import SQLAlchemyStorage

        # This trigger Options parsing because Gateway.register_all() uses Logging
        return SQLAlchemyStorage(
            platform_config = storage_config,
            router = router
        )
    else:
        raise ValueError("Invalid STORAGE_DEFAULT_GATEWAY constant (%s)" % STORAGE_DEFAULT_GATEWAY)

def install_default_storage(router):
    """
    Install the default Storage on a router.
    Args:
        router: A Router instance.
    Raises:
        Exception in case of failure.
    """
    import json
    from manifold.core.local     import LOCAL_NAMESPACE
    from manifold.util.constants import STORAGE_DEFAULT_ANNOTATION, STORAGE_DEFAULT_CONFIG, STORAGE_DEFAULT_GATEWAY, STORAGE_SQLA_FILENAME

    try:
        MANIFOLD_STORAGE = make_storage(
            STORAGE_DEFAULT_GATEWAY,
            json.loads(STORAGE_DEFAULT_CONFIG),
            router
        )
    except Exception, e:
        from manifold.util.log import Log

        Log.warning("Running Manifold without Storage due to the following Exception")
        Log.warning("%s" % traceback.format_exc())
        MANIFOLD_STORAGE = None

    # Ensure storage exists
    try:
        ensure_writable_directory(os.path.dirname(STORAGE_SQLA_FILENAME))
    except Exception, e:
        raise RuntimeError("Unable to create the default Storage directory (%s) (%s)" % (
            STORAGE_DEFAULT_GATEWAY,
            STORAGE_DEFAULT_CONFIG
        ))

    # Install storage on the Router
    storage_config = json.loads(STORAGE_DEFAULT_CONFIG)
    ok = router.add_platform(LOCAL_NAMESPACE, STORAGE_DEFAULT_GATEWAY, storage_config)

    if not ok:
        raise RuntimeError("Unable to install the default Storage (%s) (%s)" % (
            STORAGE_DEFAULT_GATEWAY,
            STORAGE_DEFAULT_CONFIG
        ))

    # Configure the Router in respect with the Storage content
    if not MANIFOLD_STORAGE:
        raise RuntimeError("Unable to install a None default Storage (%s) (%s)" % (
            STORAGE_DEFAULT_GATEWAY,
            STORAGE_DEFAULT_CONFIG
        ))
        
    try:
        MANIFOLD_STORAGE.update_router_state(
            router,
            Annotation(STORAGE_DEFAULT_ANNOTATION),
            None
        )
    except Exception, e:
        from manifold.util.log import Log
        Log.warning("Unable to setup the default Storage directory (%s) (%s)" % (
            STORAGE_DEFAULT_GATEWAY,
            STORAGE_DEFAULT_CONFIG
        ))
