#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#   This file contains the code involving both Packet and Query classes.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>
#   Loïc Baron          <loic.baron@lip6.fr>

from manifold.core.packet           import Packet
from manifold.core.query            import Query
from manifold.core.query_factory    import QueryFactory

def packet_update_query(packet,  method, *args, **kwargs):
    """
    Apply a method to a QUERY Packet.
    Args:
        packet: A Packet instance.
        method: A pointer to a function.
            *args and **kwargs are passed to method.
    """
    assert isinstance(packet, Packet)
    assert packet.get_type() in Packet.PROTOCOL_QUERY_TYPES

    packet.set_query(method(QueryFactory.from_packet(self), *args, **kwargs))
