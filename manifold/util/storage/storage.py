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

from types                          import StringTypes

from manifold.gateways              import Gateway
from manifold.core.annotation       import Annotation
from manifold.core.packet           import QueryPacket
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.type             import accepts, returns
from manifold.util.storage          import STORAGE_NAMESPACE

class Storage(object):
    def __init__(self, gateway_type, platform_config, interface = None):
        """
        Constructor.
        Args:
            gateway_type: A String containing the type of Gateway used to
                query this Manifold Storage. Example: "sqlalchemy"
            platform_config: A dictionnary containing the relevant information
                to instantiate the corresponding Gateway.
            interface: The Router on which this Storage is running.
                You may pass None if this Storage is stand-alone.
        """
        assert isinstance(gateway_type, StringTypes),\
            "Invalid gateway_type = %s (%s)" % (gateway_type, type(gateway_type))
        assert isinstance(platform_config, dict),\
            "Invalid platform_config = %s (%s)" % (platform_config, type(platform_config))
        
        self._gateway = None

        # Initialize self._storage_annotation (passed to every query run on this Storage)
        self._storage_annotation = Annotation()

    def load_gateway(self):
        # Initialize self._gateway
        Gateway.register_all()
        cls_storage = Gateway.get(gateway_type)
        if not cls_storage:
            raise Exception, "Cannot find %s Gateway, required to access Manifold Storage" % gateway_type 
        self._gateway = cls_storage(interface, STORAGE_NAMESPACE, platform_config)

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
        (ie any "STORAGE_NAMESPACE:*" object).
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

        # Check parameters
        assert gateway and isinstance(gateway, Gateway),\
            "Invalid gateway = %s (%s)" % (gateway, type(gateway))
        assert not annotation or isinstance(annotation, Annotation),\
            "Invalid annotation = %s (%s)" % (annotation, type(annotation))
        assert not ':' in query.get_from() or query.get_from().startswith(STORAGE_NAMESPACE),\
            "Invalid namespace: '%s' != '%s'" % (query.get_from(), STORAGE_NAMESPACE)

        # Enrich annotation to transport Storage's credentials
        storage_annotation = self._storage_annotation 
        if annotation:
            annotation |= storage_annotation
        else:
            annotation  = storage_annotation

        # Prepare the Receiver and the QUERY Packet
        receiver = SyncReceiver()
        packet   = QueryPacket(query, annotation, receiver)

        # Send the Packet and collect the corresponding RECORD Packets.
        gateway.add_flow(query, receiver)
        gateway.receive(packet)
        result_value = receiver.get_result_value()

        # In case of error, raise an error.
        if not result_value.is_success():
            if not error_message:
                error_message = "Error executing local query: %s" % query
            raise RuntimeError(error_message)

        # Otherwise, return the corresponding list of dicts.
        return [record.to_dict() for record in result_value["value"]]
     

