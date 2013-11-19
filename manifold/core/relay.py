# -*- coding: utf-8 -*-

from manifold.core.packet   import Packet
from manifold.core.producer import Producer
from manifold.core.consumer import Consumer

class Relay(Producer, Consumer):
    def __init__(self, producers = None, consumers = None, max_producers = None, max_consumers = None):
        Producer.__init__(self, consumers, max_consumers)
        Consumer.__init__(self, producers, max_producers)

    def relay(self, packet):
        if packet.get_type() in [Packet.TYPE_QUERY]:
            Consumer.send(self, packet)
        elif packet.get_type() in [Packet.TYPE_RECORD, Packet.TYPE_ERROR]:
            Producer.send(self, packet)

    send    = relay
    receive = relay
