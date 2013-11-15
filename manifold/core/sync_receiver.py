# -*- coding: utf-8 -*-

import threading

from manifold.core.packet       import Packet
from manifold.core.result_value import ResultValue

class SyncReceiver(object):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
        self._records = []
        self._event = threading.Event()

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        """
        """
        if packet.get_type() == Packet.TYPE_RECORD:
            record = packet
            if record.is_last():
                print "last record"
                self._event.set()

            self._records.append(packet)

        elif packet.get_type() == Packet.TYPE_ERROR:
            Log.tmp("TODO errors")

        else:
            Log.warning("Received invalid packet type")


    def get_results(self):
        self._event.wait()
        self._event.clear()
        return self.results

    def get_result_value(self):
        return ResultValue.get_success(self.get_results())
