#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.core.annotation       import Annotation
from manifold.core.packet           import QueryPacket
from manifold.core.result_value     import ResultValue
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

@returns(list)
def execute_query(destination, query, error_message):
    """
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

