#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# QueryFactory creates a Query instance from a packet or from a dict
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Loïc Baron          <loic.baron@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>
#   Jordan Augé         <jordan.auge@lip6.fr>

from manifold.core.packet           import Packet
from manifold.core.packet_util      import packet_update_action
from manifold.core.query            import ACTION_GET
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

class PacketFactory(object):
    """
    PacketFactory creates a Packet instance.
    """
    @staticmethod
    @returns(Packet)
    def query_get(src_addr, dst_addr):
        """
        Create a QUERY Packet with action == ACTION_GET
        Args:
            src_addr: The source Address.
            dst_addr: The destination Address.
        Returns:
            A Packet instance.
        """
        packet = Packet(protocol = Packet.PROTOCOL_QUERY)
        Log.tmp(src_addr)
        Log.tmp(dst_addr)
        packet.set_source(src_addr)
        packet.set_destination(dst_addr)
        packet_update_action(packet, ACTION_GET)
        return packet
