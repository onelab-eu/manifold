class FlowEntry(object):
    def __init__(self, receiver, last, timestamp, timeout):
        self._receiver  = receiver
        self._last      = last
        self._timestamp = timestamp
        self._timeout   = timeout
    #    self._expired   = False

    def get_receiver(self):
        return self._receiver

    def is_last(self):
        return self._last

    def get_timestamp(self):
        return self._timestamp

    def set_timestamp(self, timestamp):
        self._timestamp = timestamp

    def get_timeout(self):
        return self._timeout

    #def set_expired(self, expired = True):
    #    self._expired = True

    #def is_expired(self):
    #    return self._expired

