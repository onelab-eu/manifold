#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# SyncReceiver class.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import threading
from types                      import StringTypes

from manifold.core.consumer     import Consumer
from manifold.core.packet       import Packet
from manifold.core.record       import Records, LastRecord
from manifold.core.result_value import ResultValue
from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

class SyncReceiver(Consumer):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
        Consumer.__init__(self)
        self._records = Records()
        self._event = threading.Event()

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        """
        """
        if packet.get_type() == Packet.TYPE_RECORD:
            if packet.is_last():
                self._event.set()
                return

            self._records.append(packet)

        elif packet.get_type() == Packet.TYPE_ERROR:
            Log.error(packet.get_message())
            self._records.append(LastRecord())

        else:
            Log.warning("Received invalid Packet type (%s)" % packet)

    @returns(list)
    def get_results(self):
        self._event.wait()
        self._event.clear()
        return self._records.to_list()

    @returns(ResultValue)
    def get_result_value(self):
        return ResultValue.get_success(self.get_results())

    @returns(list)
    def get_records(self):
        # XXX We should inspect the return code of get_result_value()
        return self.get_results()
