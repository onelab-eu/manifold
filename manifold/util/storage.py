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

from manifold.gateways              import Gateway
from manifold.core.annotation       import Annotation
from manifold.core.packet           import QueryPacket
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.type             import accepts, returns

STORAGE_GATEWAY   = "sqlalchemy"
STORAGE_URL       = "sqlite:////var/myslice/db.sqlite?check_same_thread=False"
STORAGE_CONFIG    = {"url" : STORAGE_URL}
STORAGE_NAMESPACE = "local"

@returns(Gateway)
def make_storage(interface, storage_config = STORAGE_CONFIG):
    """
    Create a Gateway instance allowing to query the Manifold Storage
    Args:
        interface: The Interface instance on which the Storage Gateway is running.
    Returns:
        A Gateway instance allowing to query the Manifold Storage
    """
    cls_storage = Gateway.get(STORAGE_GATEWAY)
    if not cls_storage:
        raise Exception, "Cannot find %s Gateway, required to access Manifold Storage " % STORAGE_GATEWAY
    return cls_storage(interface, STORAGE_NAMESPACE, STORAGE_CONFIG)

@returns(list)
def storage_execute(storage, query, annotation = None, error_message = None):
    """
    Execute a Query related to the Manifold Storage
    (ie any "local:*" object).
    Args:
        storage: A Gateway instance allowing to query the Manifold Storage.
        query: A Query. query.get_from() should start with "local:".
        annotation: An Annotation instance related to Query or None.
        error_message: A String containing the error_message that must
            be written in case of failure.
    Raises:
        Exception: if the Query does not succeed.
    Returns:
        A list of Records.            
    """
    assert isinstance(storage, Gateway),\
        "Invalid storage = %s (%s)" % (storage, type(storage))
    assert not annotation or isinstance(annotation, Annotation),\
        "Invalid annotation = %s (%s)" % (annotation, type(annotation))
    assert not ':' in query.get_from() or query.get_from().startswith(STORAGE_NAMESPACE),\
        "Invalid namespace: '%s' != '%s'" % (query.get_from(), STORAGE_NAMESPACE)

    receiver = SyncReceiver()
    packet   = QueryPacket(query, annotation, receiver)

    receiver.set_producer(storage)
    storage.receive(packet)
    result_value = receiver.get_result_value()

    if not result_value.is_success():
        if not error_message:
            error_message = "Error executing local query: %s" % query
        raise Exception, error_message

    return result_value["value"]
        

