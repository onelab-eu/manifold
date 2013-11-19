# -*- coding: utf-8 -*-

import threading

from manifold.core.packet       import Packet
from manifold.core.record       import Records
from manifold.core.result_value import ResultValue
from manifold.util.log          import Log

class SyncReceiver(object):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
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
            Log.tmp("TODO errors")

        else:
            Log.warning("Received invalid packet type")


    def get_results(self):
        self._event.wait()
        self._event.clear()
        return self._records.to_list()

    def get_result_value(self):
        return ResultValue.get_success(self.get_results())

    def get_records(self):
        # XXX We should inspect the return code of get_result_value()
        return self.get_results()
