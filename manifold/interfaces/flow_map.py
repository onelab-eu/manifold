import time

from manifold.util.reactor_thread import ReactorThread

from flow_entry import FlowEntry

TIMEOUT_PER_TTL = 0.25
DEFAULT_TIMEOUT = 4

class FlowMap(object):
    def __init__(self, interface):
        self._interface = interface

        self._map  = dict()  # hash by flow
        self._list = list()  # sort by expiry time

        self._timer_id = None
        self._on_tick()

    def terminate(self):
        self._stop_timer()

    def add_receiver(self, packet, receiver):
        now = time.time()

        timeout = DEFAULT_TIMEOUT - packet.get_ttl() * TIMEOUT_PER_TTL
        if timeout < TIMEOUT_PER_TTL:
            timeout = TIMEOUT_PER_TTL

        flow = packet.get_flow().get_reverse()
        last = packet.is_last()

        self._map[flow] = FlowEntry(receiver, last, now, timeout)

        # With a default timeout, we always push back new entries, and no need
        # to reschedule
        if not self._list:
            self._set_timer(timeout)

        # Insert in the list by expiration time
        expiry = now + timeout
        if not flow in self._list:
            for pos, cur_flow in enumerate(self._list):
                cur_entry = self._map[cur_flow]
                if cur_entry.get_timestamp() + cur_entry.get_timeout() > expiry:
                    self._list.insert(pos, flow)
                    return 
            self._list.append(flow)

    def get(self, packet):
        flow = packet.get_flow()

        flow_entry = self._map.get(flow)
        if not flow_entry:
            return None

        return flow_entry

    def delete(self, packet):
        flow = packet.get_flow()
        del self._map[flow]
        if flow in self._list:
            self._list.remove(flow)
            self._reschedule()

    def _expire_flow(self, flow):
        record = TimeoutErrorPacket(message='Flow timeout on interface %r : %r' % (self, flow,))
        record.set_source(flow.get_source())
        record.set_destination(flow.get_destination())
        record._ingress = self._interface.get_address()

        receiver = self._map[flow].get_receiver()

        # We delete instead of expiring, cf README.architecture
        del self._map[flow]

        # XXX Code duplicated
        if receiver:
            receiver.receive(record)

    def _expire_flows(self):
        now = time.time()
        for flow in self._list:
            flow_entry = self._map[flow]
            delay = flow_entry.get_timeout() - now + flow_entry.get_timestamp()
            if delay < 0:
                del self._list[0]
                self._expire_flow(flow)
            else:
                return delay
        return 0 # no more flows

    def _reschedule(self):
        self._stop_timer()
        self._on_tick()

    def _on_tick(self, *args, **kwargs):
        #print "tick!", args, kwargs
        delay = self._expire_flows()
        if delay > 0:
            self._set_timer(delay)

    def _set_timer(self, delay):
        self._timer_id = ReactorThread().callLater(delay, self._on_tick, None)

    def _stop_timer(self):
        if self._timer_id:
            self._timer_id.cancel()
