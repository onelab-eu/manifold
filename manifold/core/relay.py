# -*- coding: utf-8 -*-

from manifold.core.producer import Producer
from manifold.core.consumer import Consumer

class Relay(Producer, Consumer):
    def __init__(self, producers = None, consumers = None, max_producers = None, max_consumers = None):
        Producer.__init__(self, consumers, max_consumers)
        Consumer.__init__(self, producers, max_producers)

    def send(self, packet):
        if packet.get_type() in [Packet.TYPE_QUERY]:
            Producer.send(self, packet)
        elif packet.get_type() in [Packet.TYPE_RECORD, Packet.TYPE_ERROR]:
            Consumer.send(self, packet)
