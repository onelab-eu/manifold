#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.core.annotation       import Annotation
from manifold.core.local            import LOCAL_NAMESPACE
from manifold.core.packet           import QueryPacket
from manifold.core.result_value     import ResultValue
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

@returns(list)
def execute_query(destination, query, error_message):
    """
    Forward a Query to a given destination.
    Args:
        destination: For instance a Gateway.
        query: A Query instance
        error_message: A String instance
    Returns:
        The corresponding list of Record.
    """
    # XXX We should benefit from caching if rules allows for it possible
    # XXX LOCAL

    if error_message:
        Log.warning("error_message not taken into account")

    # Build a query packet
    receiver = SyncReceiver()
    packet = QueryPacket(query, Annotation(), receiver)
    destination.receive(packet) # process_query_packet(packet)

    # This code is blocking
    result_value = receiver.get_result_value()
    assert isinstance(result_value, ResultValue),\
        "Invalid result_value = %s (%s)" % (result_value, type(result_value))
    return result_value.get_all().to_dict_list()

ERR_STORAGE = "Failed to execute this local query: %(query)s"

@returns(list)
def execute_local_query(query, error_message = ERR_STORAGE):
    """
    Forward a Query to the Manifold Storage.
    Args:
        query: A Query instance.
        error_message: A String instance.
    Returns:
        The corresponding list of Record.
    """
    from manifold.bin.config import MANIFOLD_STORAGE
    query.set_namespace(LOCAL_NAMESPACE)
    return execute_query(MANIFOLD_STORAGE.get_gateway(), query, error_message % locals())
